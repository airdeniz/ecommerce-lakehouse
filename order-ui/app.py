"""
Manual insert UI for the source OLTP database (Postgres).

A tiny Flask app that writes directly to the `ecommerce` Postgres, exactly like
a real application would. It is completely independent of the CDC pipeline:
Debezium/Kafka/PySpark can be stopped and this still inserts. When Connect
resumes, Postgres' replication slot replays every insert made here, so a single
manual order flows all the way to bronze/silver/gold — ideal for live demos.

Panels:
  - one raw insert form per source table (users, products, inventory, orders,
    order_items),
  - a "realistic order" panel (pick a user + products -> order + items +
    inventory decrement, in one transaction, prices taken from the DB),
  - a "10 random orders" button that mimics the generator's burst.
"""
import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
import redis as redis_lib
from flask import Flask, request, jsonify

DSN = os.getenv(
    "PG_DSN",
    "host=postgres port=5432 dbname=ecommerce user=postgres password=postgres",
)

# Read-only view onto the real-time serving layer maintained by
# realtime-aggregator. The UI only READS these pre-aggregated counters; it never
# writes them (the aggregator owns the write path).
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
TRENDING_WINDOW_MIN = int(os.getenv("TRENDING_WINDOW_MIN", "60"))

app = Flask(__name__)

_redis = None


def get_redis():
    """Lazily-built shared Redis client (decoded strings)."""
    global _redis
    if _redis is None:
        _redis = redis_lib.from_url(REDIS_URL, decode_responses=True)
    return _redis


def compute_trending(r, top=10):
    """Top products by units sold over the last TRENDING_WINDOW_MIN minutes.

    The aggregator writes one ZSET per minute (`trending:min:<YYYYMMDDHHMM>`); the
    "sliding window" is computed here on read by summing the last N buckets. This
    keeps the read path side-effect free — it never mutates Redis.

    The window is anchored on `metrics:last_event_min` (the newest event minute the
    aggregator has seen), not wall-clock now(): the reader and the CDC event
    stream can live on different clocks, and anchoring on the data's own time also
    keeps trending correct while the aggregator replays a backlog on startup.
    """
    anchor = r.get("metrics:last_event_min")
    try:
        base = datetime.strptime(anchor, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        base = datetime.now(timezone.utc)
    keys = [f"trending:min:{(base - timedelta(minutes=i)).strftime('%Y%m%d%H%M')}"
            for i in range(TRENDING_WINDOW_MIN)]
    pipe = r.pipeline()
    for k in keys:
        pipe.zrange(k, 0, -1, withscores=True)
    scores = {}
    for res in pipe.execute():
        for member, score in res:
            scores[member] = scores.get(member, 0.0) + score
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])[:top]
    if not ranked:
        return []
    names = r.hmget("dim:product_name", [m for m, _ in ranked])
    return [{"product_id": int(m), "name": names[i] or f"#{m}", "units": int(s)}
            for i, (m, s) in enumerate(ranked)]

# Whitelisted insertable columns per table (auto PKs / defaults are excluded).
TABLES = {
    "users": ["full_name", "city"],
    "products": ["product_id", "name", "category", "price"],
    "inventory": ["product_id", "stock_qty"],
    "orders": ["user_id", "status", "total_amount"],
    "order_items": ["order_id", "product_id", "quantity", "unit_price"],
}
PK = {
    "users": "user_id",
    "products": "product_id",
    "inventory": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}
# Type coercion so form strings land as the right Postgres type.
COLTYPES = {
    "full_name": str, "city": str, "name": str, "category": str, "status": str,
    "product_id": int, "user_id": int, "order_id": int,
    "quantity": int, "stock_qty": int,
    "price": float, "unit_price": float, "total_amount": float,
}


# Steady-state status mix for the "random orders" burst, so a single click seeds
# a realistic spread across the whole lifecycle instead of only CREATED/PAID/
# CANCELLED. (weight is relative.) The generator walks orders through the
# lifecycle over time; this is the one-shot snapshot equivalent.
RANDOM_STATUS_MIX = [
    ("DELIVERED", 42), ("SHIPPED", 12), ("PREPARING", 8), ("PAID", 8),
    ("CREATED", 8), ("CANCELLED", 12), ("RETURNED", 5), ("REFUNDED", 5),
]


def get_conn():
    return psycopg2.connect(DSN)


def coerce(col, value):
    caster = COLTYPES.get(col, str)
    if value is None or value == "":
        raise ValueError(f"'{col}' bos olamaz")
    return caster(value)


@app.get("/api/meta")
def meta():
    """Dropdown data + row counts for the UI."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT user_id, full_name, city FROM users ORDER BY user_id LIMIT 500")
        users = [{"user_id": r[0], "label": f"{r[0]} · {r[1]} ({r[2]})"} for r in cur.fetchall()]
        cur.execute("SELECT product_id, name, price FROM products ORDER BY product_id")
        products = [{"product_id": r[0], "name": r[1], "price": float(r[2])} for r in cur.fetchall()]
        cur.execute("SELECT order_id FROM orders ORDER BY order_id DESC LIMIT 30")
        orders = [r[0] for r in cur.fetchall()]
        counts = {}
        for t in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
    return jsonify({"users": users, "products": products, "recent_orders": orders, "counts": counts})


@app.post("/api/insert/<table>")
def insert(table):
    """Generic single-row insert into one whitelisted table."""
    if table not in TABLES:
        return jsonify({"ok": False, "error": f"bilinmeyen tablo: {table}"}), 400
    body = request.get_json(force=True) or {}
    cols = TABLES[table]
    try:
        values = [coerce(c, body.get(c)) for c in cols]
    except (ValueError, TypeError) as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) RETURNING {PK[table]}"
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, values)
            new_id = cur.fetchone()[0]
            conn.commit()
    except psycopg2.Error as e:
        return jsonify({"ok": False, "error": str(e.pgerror or e).strip()}), 400
    return jsonify({"ok": True, "table": table, "pk": PK[table], "id": new_id})


def _create_order(cur, user_id, items):
    """order + order_items + inventory decrement, in the caller's transaction.

    items: list of (product_id, quantity). Unit price is read from products so
    the order total is authoritative (same approach the generator uses).
    """
    total = 0.0
    priced = []
    for product_id, qty in items:
        cur.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"product_id {product_id} yok")
        price = float(row[0])
        total += price * qty
        priced.append((product_id, qty, price))

    cur.execute(
        "INSERT INTO orders (user_id, status, total_amount) VALUES (%s, 'CREATED', %s) RETURNING order_id",
        (user_id, round(total, 2)),
    )
    order_id = cur.fetchone()[0]
    for product_id, qty, price in priced:
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, qty, price),
        )
        cur.execute(
            "UPDATE inventory SET stock_qty = stock_qty - %s WHERE product_id = %s",
            (qty, product_id),
        )
    return order_id, round(total, 2), len(priced)


@app.post("/api/order")
def create_order():
    """Realistic single order: order + items + inventory, one transaction."""
    body = request.get_json(force=True) or {}
    try:
        user_id = int(body["user_id"])
        items = [(int(i["product_id"]), int(i["quantity"])) for i in body.get("items", [])]
    except (KeyError, ValueError, TypeError):
        return jsonify({"ok": False, "error": "gecersiz user_id / items"}), 400
    if not items:
        return jsonify({"ok": False, "error": "en az bir urun ekleyin"}), 400
    status = body.get("status", "CREATED")
    try:
        with get_conn() as conn, conn.cursor() as cur:
            order_id, total, n = _create_order(cur, user_id, items)
            if status in ("PAID", "CANCELLED"):
                cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (status, order_id))
            conn.commit()
    except (psycopg2.Error, ValueError) as e:
        return jsonify({"ok": False, "error": str(getattr(e, "pgerror", None) or e).strip()}), 400
    return jsonify({"ok": True, "order_id": order_id, "total": total, "items": n, "status": status})


@app.post("/api/random")
def random_orders():
    """Generate N realistic random orders (default 10), like a generator burst."""
    count = int(request.args.get("count", 10))
    count = max(1, min(count, 100))
    created = []
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            user_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT product_id FROM products")
            product_ids = [r[0] for r in cur.fetchall()]
            for _ in range(count):
                user_id = random.choice(user_ids)
                k = random.randint(1, 4)
                chosen = random.sample(product_ids, k=min(k, len(product_ids)))
                items = [(pid, random.randint(1, 4)) for pid in chosen]
                order_id, total, n = _create_order(cur, user_id, items)
                # Draw a realistic lifecycle status; anything past CREATED emits an
                # extra 'u' event, exercising the CDC update path.
                status = random.choices(
                    [s for s, _ in RANDOM_STATUS_MIX],
                    weights=[w for _, w in RANDOM_STATUS_MIX], k=1,
                )[0]
                if status != "CREATED":
                    cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (status, order_id))
                created.append({"order_id": order_id, "user_id": user_id, "total": total, "items": n, "status": status})
            conn.commit()
    except (psycopg2.Error, ValueError) as e:
        return jsonify({"ok": False, "error": str(getattr(e, "pgerror", None) or e).strip()}), 400
    return jsonify({"ok": True, "created": created})


@app.get("/api/order/<int:order_id>")
def order_detail(order_id):
    """Full order view — order + customer + line items, like a source-app screen."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.order_id, o.user_id, o.status, o.total_amount, o.created_at,
                   u.full_name, u.city
            FROM orders o JOIN users u ON u.user_id = o.user_id
            WHERE o.order_id = %s
            """,
            (order_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"ok": False, "error": f"siparis {order_id} bulunamadi"}), 404
        order = {
            "order_id": row[0], "user_id": row[1], "status": row[2],
            "total_amount": float(row[3]), "created_at": str(row[4]),
            "full_name": row[5], "city": row[6],
        }
        cur.execute(
            """
            SELECT oi.order_item_id, oi.product_id, p.name, p.category,
                   oi.quantity, oi.unit_price
            FROM order_items oi JOIN products p ON p.product_id = oi.product_id
            WHERE oi.order_id = %s
            ORDER BY oi.order_item_id
            """,
            (order_id,),
        )
        items = [{
            "product_id": r[1], "name": r[2], "category": r[3],
            "quantity": r[4], "unit_price": float(r[5]),
            "line_total": round(float(r[5]) * r[4], 2),
        } for r in cur.fetchall()]
    return jsonify({"ok": True, "order": order, "items": items})


@app.get("/api/live")
def live():
    """Live pre-aggregated metrics from the Redis serving layer (read-only)."""
    try:
        r = get_redis()
        total = int(r.get("metrics:orders:total") or 0)
        revenue = float(r.get("metrics:revenue:net") or 0.0)
        events = int(r.get("metrics:events:processed") or 0)
        updated = r.get("metrics:updated_at")
        status = {k: int(v) for k, v in (r.hgetall("metrics:orders:status") or {}).items()}
        by_city = sorted(
            ({"city": k, "count": int(v)} for k, v in (r.hgetall("metrics:orders:by_city") or {}).items()),
            key=lambda x: -x["count"],
        )
        trending = compute_trending(r, top=10)
    except redis_lib.exceptions.RedisError as e:
        return jsonify({"ok": False, "error": f"serving layer unavailable: {e}"}), 503
    delivered_rate = round(status.get("DELIVERED", 0) / total * 100) if total else 0
    return jsonify({
        "ok": True, "total_orders": total, "revenue_net": round(revenue, 2),
        "events": events, "updated_at": updated, "status": status,
        "delivered_rate": delivered_rate,
        "by_city": by_city, "trending": trending, "window_min": TRENDING_WINDOW_MIN,
    })


@app.get("/")
def index():
    return INDEX_HTML


INDEX_HTML = """<!doctype html>
<html lang="tr"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kaynak Sistem · Manuel Insert</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
         background:#0f1420; color:#e6e9ef; line-height:1.45; }
  header { padding:18px 24px; background:#161d2e; border-bottom:1px solid #26304a; }
  h1 { margin:0; font-size:18px; }
  .sub { color:#8b97b0; font-size:13px; margin-top:4px; }
  main { max-width:1100px; margin:0 auto; padding:20px; }
  .grid { display:grid; gap:16px; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); }
  .tabs { display:flex; gap:4px; max-width:1100px; margin:14px auto 0; padding:0 20px; }
  .tab { background:#161d2e; color:#9aa6c0; border:1px solid #26304a; border-bottom:none;
         border-radius:9px 9px 0 0; padding:10px 18px; margin:0; font-weight:600; }
  .tab.active { background:#1d273e; color:#e6e9ef; }
  .tabpanel.hidden { display:none; }
  .detail { max-width:660px; margin:0 auto; }
  .badge { display:inline-block; padding:3px 11px; border-radius:20px; font-size:12px; font-weight:700; }
  .b-CREATED{background:#334155;color:#cbd5e1;} .b-PAID{background:#1e3a8a;color:#93c5fd;}
  .b-PREPARING{background:#0c4a6e;color:#7dd3fc;} .b-SHIPPED{background:#4c1d95;color:#c4b5fd;}
  .b-DELIVERED{background:#14532d;color:#4ade80;} .b-CANCELLED{background:#4c1d24;color:#f87171;}
  .b-RETURNED{background:#78350f;color:#fcd34d;} .b-REFUNDED{background:#4c0519;color:#fda4af;}
  .kv { display:flex; gap:20px; flex-wrap:wrap; color:#9aa6c0; font-size:13px; margin:8px 0 2px; }
  .kv b { color:#e6e9ef; }
  table.items-tbl { width:100%; border-collapse:collapse; margin-top:12px; font-size:13px; }
  table.items-tbl th, table.items-tbl td { padding:7px 8px; border-bottom:1px solid #26304a; text-align:left; }
  table.items-tbl th { color:#9aa6c0; font-weight:600; }
  table.items-tbl .num { text-align:right; }
  tfoot td { font-weight:700; color:#e6e9ef; border-top:2px solid #2c3550; }
  .card { background:#161d2e; border:1px solid #26304a; border-radius:10px; padding:16px; }
  .card h2 { margin:0 0 12px; font-size:15px; display:flex; align-items:center; gap:8px; }
  .card.wide { grid-column: 1 / -1; }
  label { display:block; font-size:12px; color:#9aa6c0; margin:8px 0 3px; }
  input, select { width:100%; padding:8px 10px; background:#0f1420; border:1px solid #2c3550;
          border-radius:7px; color:#e6e9ef; font-size:13px; }
  button { cursor:pointer; border:none; border-radius:7px; padding:9px 14px; font-size:13px;
           font-weight:600; color:#fff; background:#3b82f6; margin-top:12px; }
  button:hover { background:#2f6fd6; }
  button.big { background:#16a34a; font-size:15px; padding:13px; width:100%; }
  button.big:hover { background:#128a3e; }
  button.ghost { background:#2c3550; }
  .row { display:flex; gap:8px; align-items:end; }
  .row > * { flex:1; }
  .items .line { display:flex; gap:6px; margin-bottom:6px; }
  .items .line select { flex:3; } .items .line input { flex:1; }
  .items .line button { margin:0; padding:6px 10px; background:#7c2d3a; }
  #log { grid-column:1/-1; background:#0b0f18; border:1px solid #26304a; border-radius:10px;
         padding:12px 14px; font-family: ui-monospace, monospace; font-size:12.5px;
         max-height:230px; overflow:auto; }
  .ok { color:#4ade80; } .err { color:#f87171; } .muted{color:#8b97b0;}
  .counts { display:flex; flex-wrap:wrap; gap:8px 16px; font-size:12px; color:#9aa6c0; margin-top:6px;}
  .counts b { color:#e6e9ef; }
  .stats { display:grid; gap:12px; grid-template-columns: repeat(auto-fit, minmax(170px,1fr)); }
  .stat { background:#161d2e; border:1px solid #26304a; border-radius:10px; padding:16px; }
  .stat .n { font-size:26px; font-weight:800; color:#e6e9ef; }
  .stat .l { font-size:12px; color:#9aa6c0; margin-top:4px; }
  .live-grid { display:grid; gap:16px; grid-template-columns: 1fr 1fr; margin-top:16px; }
  @media (max-width:760px){ .live-grid{ grid-template-columns:1fr; } }
  .bar-row { display:flex; align-items:center; gap:10px; margin:7px 0; font-size:13px; }
  .bar-row .name { flex:0 0 128px; color:#cbd5e1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .bar-row .track { flex:1; background:#0f1420; border-radius:6px; height:9px; }
  .bar-row .bar { display:block; height:9px; border-radius:6px; background:#3b82f6; }
  .bar-row .val { flex:0 0 44px; text-align:right; color:#9aa6c0; }
  .live-updated { font-size:12px; color:#8b97b0; margin-top:12px; }
</style></head>
<body>
<header>
  <h1>🛒 Kaynak Sistem — Manuel Insert Paneli</h1>
  <div class="sub">Postgres <code>ecommerce</code> veritabanına doğrudan yazar. Generator kapalı olsa bile çalışır; pipeline açılınca CDC ile akar.</div>
  <div class="counts" id="counts"></div>
</header>
<div class="tabs">
  <button class="tab active" data-tab="insert" onclick="showTab('insert')">✍️ Manuel Insert</button>
  <button class="tab" data-tab="lookup" onclick="showTab('lookup')">🔎 Sipariş Detay</button>
  <button class="tab" data-tab="live" onclick="showTab('live')">📊 Canlı Metrikler</button>
</div>
<main>
  <section id="tab-insert" class="tabpanel grid">
  <div class="card wide">
    <h2>🎲 Hızlı demo</h2>
    <div class="row">
      <div>
        <label>Kaç adet rastgele sipariş?</label>
        <input type="number" id="randCount" value="10" min="1" max="100">
      </div>
      <button class="big" style="flex:2" onclick="randomOrders()">🎲 Rastgele sipariş oluştur (order + items + stok)</button>
    </div>
  </div>

  <div class="card wide">
    <h2>🧾 Gerçekçi sipariş oluştur</h2>
    <label>Müşteri (user)</label>
    <select id="ord_user"></select>
    <label>Ürünler</label>
    <div class="items" id="ord_items"></div>
    <button class="ghost" onclick="addItemLine()">+ ürün ekle</button>
    <label>Durum</label>
    <select id="ord_status">
      <option>CREATED</option><option>PAID</option><option>PREPARING</option>
      <option>SHIPPED</option><option>DELIVERED</option><option>CANCELLED</option>
      <option>RETURNED</option><option>REFUNDED</option>
    </select>
    <button onclick="submitOrder()">Siparişi oluştur</button>
  </div>

  <div class="card">
    <h2>👤 users</h2>
    <label>full_name</label><input id="u_name" placeholder="Ad Soyad">
    <label>city</label><input id="u_city" placeholder="Istanbul">
    <button onclick="rawInsert('users',{full_name:v('u_name'),city:v('u_city')})">Ekle</button>
  </div>

  <div class="card">
    <h2>📦 products</h2>
    <label>product_id</label><input id="p_id" type="number" placeholder="51">
    <label>name</label><input id="p_name" placeholder="Ürün adı">
    <label>category</label><input id="p_cat" placeholder="Elektronik">
    <label>price</label><input id="p_price" type="number" step="0.01" placeholder="199.90">
    <button onclick="rawInsert('products',{product_id:v('p_id'),name:v('p_name'),category:v('p_cat'),price:v('p_price')})">Ekle</button>
  </div>

  <div class="card">
    <h2>🏷️ inventory</h2>
    <label>product_id</label><input id="i_pid" type="number" placeholder="51">
    <label>stock_qty</label><input id="i_qty" type="number" placeholder="100">
    <button onclick="rawInsert('inventory',{product_id:v('i_pid'),stock_qty:v('i_qty')})">Ekle</button>
  </div>

  <div class="card">
    <h2>🧾 orders (ham)</h2>
    <label>user_id</label><input id="o_uid" type="number" placeholder="1">
    <label>status</label><input id="o_status" value="CREATED">
    <label>total_amount</label><input id="o_total" type="number" step="0.01" placeholder="1299.90">
    <button onclick="rawInsert('orders',{user_id:v('o_uid'),status:v('o_status'),total_amount:v('o_total')})">Ekle</button>
  </div>

  <div class="card">
    <h2>➕ order_items (ham)</h2>
    <label>order_id</label><input id="oi_oid" type="number" placeholder="1">
    <label>product_id</label><input id="oi_pid" type="number" placeholder="1">
    <label>quantity</label><input id="oi_qty" type="number" placeholder="2">
    <label>unit_price</label><input id="oi_price" type="number" step="0.01" placeholder="1299.90">
    <button onclick="rawInsert('order_items',{order_id:v('oi_oid'),product_id:v('oi_pid'),quantity:v('oi_qty'),unit_price:v('oi_price')})">Ekle</button>
  </div>

  <div id="log"><span class="muted">İşlem kayıtları burada görünür…</span></div>
  </section>

  <section id="tab-lookup" class="tabpanel hidden">
    <div class="card detail">
      <h2>🔎 Sipariş sorgula</h2>
      <div class="sub" style="margin-bottom:6px">order_id gir, kaynak sistemdeki sipariş detayını gör.</div>
      <div class="row">
        <div><label>order_id</label><input type="number" id="lk_id" placeholder="örn. 5" onkeydown="if(event.key==='Enter')lookup()"></div>
        <button style="flex:0 0 auto" onclick="lookup()">Getir</button>
      </div>
      <div id="lk_result" style="margin-top:16px"><span class="muted">Bir order_id girin…</span></div>
    </div>
  </section>

  <section id="tab-live" class="tabpanel hidden">
    <div class="sub" style="margin-bottom:12px">
      Redis <b>serving layer</b>'dan canlı okunur (realtime-aggregator, Kafka → Redis). Warehouse/dbt değil; 2 sn'de bir yenilenir.
    </div>
    <div class="stats" id="live_stats"><span class="muted">Yükleniyor…</span></div>
    <div class="card" style="margin-top:16px"><h2>📦 Sipariş statü dağılımı (yaşam döngüsü)</h2><div id="live_status"></div></div>
    <div class="live-grid">
      <div class="card"><h2>🏙️ Şehir bazında sipariş (canlı)</h2><div id="live_city"></div></div>
      <div class="card"><h2>🔥 Trend ürünler <span class="muted" id="live_window"></span></h2><div id="live_trending"></div></div>
    </div>
    <div class="live-updated" id="live_updated"></div>
  </section>
</main>

<script>
let META = {users:[], products:[]};
const v = id => document.getElementById(id).value;
function log(msg, cls) {
  const el = document.getElementById('log');
  const t = new Date().toLocaleTimeString();
  el.innerHTML = `<div class="${cls||''}">[${t}] ${msg}</div>` + el.innerHTML;
}
async function loadMeta() {
  const r = await fetch('/api/meta'); META = await r.json();
  const us = document.getElementById('ord_user');
  us.innerHTML = META.users.map(u=>`<option value="${u.user_id}">${u.label}</option>`).join('');
  const c = META.counts;
  document.getElementById('counts').innerHTML =
    Object.entries(c).map(([k,n])=>`${k}: <b>${n}</b>`).join('');
  if (!document.querySelector('#ord_items .line')) addItemLine();
}
function productOptions(sel) {
  return META.products.map(p=>`<option value="${p.product_id}">${p.product_id} · ${p.name} (${p.price}₺)</option>`).join('');
}
function addItemLine() {
  const wrap = document.getElementById('ord_items');
  const div = document.createElement('div'); div.className='line';
  div.innerHTML = `<select>${productOptions()}</select>
                   <input type="number" value="1" min="1" title="adet">
                   <button onclick="this.parentNode.remove()">✕</button>`;
  wrap.appendChild(div);
}
async function post(url, body) {
  const r = await fetch(url, {method:'POST', headers:{'Content-Type':'application/json'},
                             body: body?JSON.stringify(body):null});
  return r.json();
}
async function rawInsert(table, data) {
  const res = await post('/api/insert/'+table, data);
  if (res.ok) { log(`✔ ${table}: ${res.pk}=${res.id} eklendi`, 'ok'); loadMeta(); }
  else log(`✕ ${table}: ${res.error}`, 'err');
}
async function submitOrder() {
  const items = [...document.querySelectorAll('#ord_items .line')].map(l=>({
     product_id: l.querySelector('select').value, quantity: l.querySelector('input').value }));
  const res = await post('/api/order', {user_id:v('ord_user'), items, status:v('ord_status')});
  if (res.ok) { log(`✔ sipariş #${res.order_id} · ${res.items} ürün · ${res.total}₺ · ${res.status}`, 'ok'); loadMeta(); }
  else log(`✕ sipariş: ${res.error}`, 'err');
}
async function randomOrders() {
  const n = document.getElementById('randCount').value;
  log(`⏳ ${n} rastgele sipariş oluşturuluyor…`, 'muted');
  const res = await post('/api/random?count='+n);
  if (res.ok) {
    res.created.forEach(o => log(`✔ #${o.order_id} · user ${o.user_id} · ${o.items} ürün · ${o.total}₺ · ${o.status}`, 'ok'));
    log(`🎲 ${res.created.length} sipariş oluşturuldu`, 'ok'); loadMeta();
  } else log(`✕ ${res.error}`, 'err');
}
let liveTimer = null;
function showTab(name) {
  document.querySelectorAll('.tabpanel').forEach(p=>p.classList.add('hidden'));
  document.getElementById('tab-'+name).classList.remove('hidden');
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active', t.dataset.tab===name));
  // Poll the serving layer only while the live tab is visible.
  if (name === 'live') { refreshLive(); liveTimer = liveTimer || setInterval(refreshLive, 2000); }
  else if (liveTimer) { clearInterval(liveTimer); liveTimer = null; }
}
function bars(rows, color) {
  if (!rows.length) return '<span class="muted">veri yok</span>';
  const max = Math.max(1, ...rows.map(r=>r.val));
  return rows.map(r=>`<div class="bar-row"><span class="name" title="${r.label}">${r.label}</span>
    <span class="track"><span class="bar" style="width:${Math.round(r.val/max*100)}%${color?';background:'+color:''}"></span></span>
    <span class="val">${r.val}</span></div>`).join('');
}
async function refreshLive() {
  const stats = document.getElementById('live_stats');
  let d;
  try { d = await (await fetch('/api/live')).json(); }
  catch(e){ stats.innerHTML = '<span class="err">İstek başarısız</span>'; return; }
  if (!d.ok) { stats.innerHTML = `<span class="err">Serving layer erişilemiyor: ${d.error||''}</span>`; return; }
  const s = d.status || {};
  const n = x => (x||0).toLocaleString('tr');
  stats.innerHTML = `
    <div class="stat"><div class="n">${n(d.total_orders)}</div><div class="l">Toplam sipariş</div></div>
    <div class="stat"><div class="n">${d.revenue_net.toLocaleString('tr',{maximumFractionDigits:0})}₺</div><div class="l">Net ciro (iadeler düşülü)</div></div>
    <div class="stat"><div class="n">${n(d.delivered_rate)}%</div><div class="l">Teslim oranı</div></div>
    <div class="stat"><div class="n">${n(d.events)}</div><div class="l">İşlenen CDC olayı</div></div>`;
  // Status breakdown in canonical lifecycle order, colour-coded like the badges.
  const ORDER = ['CREATED','PAID','PREPARING','SHIPPED','DELIVERED','CANCELLED','RETURNED','REFUNDED'];
  const COLOR = {CREATED:'#64748b',PAID:'#3b82f6',PREPARING:'#0ea5e9',SHIPPED:'#8b5cf6',
                 DELIVERED:'#16a34a',CANCELLED:'#ef4444',RETURNED:'#f59e0b',REFUNDED:'#e11d48'};
  const rows = ORDER.filter(k=>s[k]).map(k=>({label:k, val:s[k], color:COLOR[k]}));
  const maxS = Math.max(1, ...rows.map(r=>r.val));
  document.getElementById('live_status').innerHTML = rows.length ? rows.map(r=>
    `<div class="bar-row"><span class="name">${r.label}</span>
      <span class="track"><span class="bar" style="width:${Math.round(r.val/maxS*100)}%;background:${r.color}"></span></span>
      <span class="val">${r.val}</span></div>`).join('') : '<span class="muted">veri yok</span>';
  document.getElementById('live_city').innerHTML =
    bars(d.by_city.map(c=>({label:c.city, val:c.count})));
  document.getElementById('live_window').textContent = `(son ${d.window_min} dk)`;
  document.getElementById('live_trending').innerHTML =
    bars(d.trending.map((t,i)=>({label:`${i+1}. ${t.name}`, val:t.units})), '#f59e0b')
    || '<span class="muted">henüz satış yok</span>';
  document.getElementById('live_updated').textContent =
    d.updated_at ? ('Son güncelleme: ' + new Date(d.updated_at).toLocaleTimeString('tr')) : '';
}
async function lookup() {
  const id = document.getElementById('lk_id').value;
  const box = document.getElementById('lk_result');
  if (!id) { box.innerHTML = '<span class="err">order_id girin</span>'; return; }
  const r = await fetch('/api/order/'+id);
  const d = await r.json();
  if (!d.ok) { box.innerHTML = `<span class="err">✕ ${d.error}</span>`; return; }
  const o = d.order;
  const rows = d.items.map(it=>`<tr><td>${it.product_id}</td><td>${it.name}</td><td>${it.category}</td>
     <td class="num">${it.quantity}</td><td class="num">${it.unit_price}₺</td><td class="num">${it.line_total}₺</td></tr>`).join('');
  box.innerHTML = `
    <div class="kv"><span style="font-size:15px">Sipariş <b>#${o.order_id}</b></span>
      <span class="badge b-${o.status}">${o.status}</span></div>
    <div class="kv"><span>Müşteri: <b>${o.full_name}</b> (user ${o.user_id})</span><span>Şehir: <b>${o.city}</b></span></div>
    <div class="kv"><span>Tarih: <b>${o.created_at}</b></span></div>
    <table class="items-tbl">
      <thead><tr><th>ürün</th><th>ad</th><th>kategori</th><th class="num">adet</th><th class="num">birim</th><th class="num">tutar</th></tr></thead>
      <tbody>${rows}</tbody>
      <tfoot><tr><td colspan="5" class="num">Toplam</td><td class="num">${o.total_amount}₺</td></tr></tfoot>
    </table>`;
}
loadMeta();
</script>
</body></html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8090)
