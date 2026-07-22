"""
Real-Time Aggregator  (Kafka -> Redis speed/serving layer)
==========================================================

This service is a Kafka CONSUMER, exactly like stock-monitor: it touches nothing
in the existing pipeline (Postgres/Debezium/Kafka/PySpark/dbt are unchanged) and
just joins the already-flowing CDC topics with a new consumer group.

WHY THIS EXISTS
---------------
Everything queryable in this project goes through either the Spark Thrift Server
(seconds-to-minutes per query) or the nightly dbt batch. That is correct for
historical analytics but far too slow for a live operational view: a dashboard
or a product page that asks "how much revenue so far today?", "orders per city
right now?", or "what's trending this minute?" cannot wait on Spark.

Real e-commerce platforms solve this with a **speed / serving layer**: a stream
processor maintains pre-computed aggregates in a low-latency key/value store so
the read path is sub-millisecond. This service is that layer, backed by Redis.

    Lakehouse (Iceberg/dbt)  =  correctness + full history, batch, slow reads
    Redis (this service)     =  live pre-aggregated counters, ms reads

The two COMPLEMENT each other; Redis never replaces the lakehouse. Bronze stays
the single source of truth (see CLAUDE.md). Redis here is a **derived cache**:
this service commits no Kafka offsets and reads every topic from `earliest` on
each start, so the entire Redis state is rebuilt from the Kafka log on every
boot. Lose Redis, restart the service, and it repopulates itself from the stream.

WHAT IT MAINTAINS IN REDIS
--------------------------
  metrics:orders:total     (int)    orders created since the first CDC event
  metrics:orders:status    (hash)   live CREATED / PAID / CANCELLED counts
  metrics:revenue:paid     (float)  summed total_amount of orders that reached PAID
  metrics:orders:by_city   (hash)   created-order count per customer city
  metrics:events:processed (int)    CDC events consumed (ops metric)
  metrics:updated_at       (str)    ISO timestamp of the last processed event
  trending:min:<YYYYMMDDHHMM> (zset) per-minute units-sold per product, TTL'd,
                                     unioned over a sliding window by the reader
  dim:user_city            (hash)   user_id -> city   (enrichment lookup)
  dim:product_name         (hash)   product_id -> name (trending display labels)

The per-city count needs a city, which lives on `users`, not on `orders`; the
trending display needs a product name, which lives on `products`, not on
`order_items`. So this service keeps small **dimension caches** in Redis, warmed
from the users/products topics before the main loop starts (a stream-table join
done the KV way) and kept fresh as those topics change.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone

sys.stdout.reconfigure(line_buffering=True)

import redis
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092,kafka2:9092,kafka3:9092")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

ORDERS_TOPIC = os.environ.get("ORDERS_TOPIC", "ecom.public.orders")
ORDER_ITEMS_TOPIC = os.environ.get("ORDER_ITEMS_TOPIC", "ecom.public.order_items")
USERS_TOPIC = os.environ.get("USERS_TOPIC", "ecom.public.users")
PRODUCTS_TOPIC = os.environ.get("PRODUCTS_TOPIC", "ecom.public.products")

# How long a per-minute trending bucket lives. The reader unions the last
# TRENDING_WINDOW_MIN buckets, so buckets need only outlive that window; the TTL
# also bounds how much trending data Redis ever holds.
TRENDING_WINDOW_MIN = int(os.environ.get("TRENDING_WINDOW_MIN", "60"))
TRENDING_TTL_S = (TRENDING_WINDOW_MIN + 10) * 60

# Redis keys this service owns. Cleared on startup so every replay rebuilds a
# clean state instead of doubling onto whatever a previous run left behind.
KEY_PREFIXES = ["metrics:", "trending:", "dim:"]


def connect_redis():
    """Connect to Redis, retrying until it is reachable (compose start order)."""
    r = redis.from_url(REDIS_URL, decode_responses=True)
    for attempt in range(30):
        try:
            r.ping()
            return r
        except redis.exceptions.RedisError as exc:
            print(f"  Redis not ready ({exc}); retrying [{attempt + 1}/30]...")
            time.sleep(2)
    raise SystemExit("Redis unreachable after 30 attempts")


def flush_owned_keys(r):
    """Delete every key this service owns so a fresh replay starts from zero."""
    for prefix in KEY_PREFIXES:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{prefix}*", count=500)
            if keys:
                r.delete(*keys)
            if cursor == 0:
                break


def deserialize(m):
    return json.loads(m.decode("utf-8")) if m else None


def event_minute(payload):
    """UTC YYYYMMDDHHMM bucket from the source commit time of a CDC event."""
    src = payload.get("source") or {}
    ts_ms = src.get("ts_ms") or payload.get("ts_ms")
    ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms else datetime.now(timezone.utc)
    return ts.strftime("%Y%m%d%H%M")


# ---------------------------------------------------------------------------
# Dimension caches (users / products) — warmed before the main loop, kept fresh.
# ---------------------------------------------------------------------------
def apply_user(r, payload):
    op = payload.get("op")
    after, before = payload.get("after"), payload.get("before")
    if op == "d":
        if before and before.get("user_id") is not None:
            r.hdel("dim:user_city", str(before["user_id"]))
        return
    if after and after.get("user_id") is not None:
        r.hset("dim:user_city", str(after["user_id"]), after.get("city") or "Unknown")


def apply_product(r, payload):
    op = payload.get("op")
    after, before = payload.get("after"), payload.get("before")
    if op == "d":
        if before and before.get("product_id") is not None:
            r.hdel("dim:product_name", str(before["product_id"]))
        return
    if after and after.get("product_id") is not None:
        r.hset("dim:product_name", str(after["product_id"]), after.get("name") or f"#{after['product_id']}")


def preload_dimensions(r):
    """Drain users + products from earliest into the dim caches, then stop.

    Runs before the main loop so an order can always resolve its city and a sold
    item its product name. `consumer_timeout_ms` makes the iterator stop once the
    backlog is drained (no committed offsets -> always starts at earliest).
    """
    print("Warming dimension caches (users, products)...")
    consumer = KafkaConsumer(
        USERS_TOPIC,
        PRODUCTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="realtime-aggregator-preload",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=8000,
        value_deserializer=deserialize,
    )
    n = 0
    for message in consumer:
        payload = (message.value or {}).get("payload")
        if not payload:
            continue
        if message.topic == USERS_TOPIC:
            apply_user(r, payload)
        else:
            apply_product(r, payload)
        n += 1
    consumer.close()
    print(f"  Dimension caches warm: {r.hlen('dim:user_city')} users, "
          f"{r.hlen('dim:product_name')} products ({n} events).")


# ---------------------------------------------------------------------------
# Fact handlers — orders (counts / revenue / city) and order_items (trending).
# ---------------------------------------------------------------------------
def handle_order(r, payload):
    op = payload.get("op")
    after, before = payload.get("after"), payload.get("before")

    # New order (create, or snapshot read of a pre-existing one).
    if op in ("c", "r") and after:
        status = after.get("status") or "CREATED"
        r.incr("metrics:orders:total")
        r.hincrby("metrics:orders:status", status, 1)
        city = r.hget("dim:user_city", str(after.get("user_id"))) or "Unknown"
        r.hincrby("metrics:orders:by_city", city, 1)
        # A snapshot row may already be PAID; count its revenue once.
        if status == "PAID" and after.get("total_amount") is not None:
            r.incrbyfloat("metrics:revenue:paid", float(after["total_amount"]))
        return

    # Status transition (CREATED -> PAID / CANCELLED).
    if op == "u" and after:
        new_status = after.get("status")
        old_status = before.get("status") if before else None
        if old_status and new_status and old_status != new_status:
            r.hincrby("metrics:orders:status", old_status, -1)
            r.hincrby("metrics:orders:status", new_status, 1)
            if new_status == "PAID" and after.get("total_amount") is not None:
                r.incrbyfloat("metrics:revenue:paid", float(after["total_amount"]))
        return

    # Delete (OLTP cleanup of a cancelled order) — keep the status hash honest.
    if op == "d" and before:
        old_status = before.get("status")
        if old_status:
            r.hincrby("metrics:orders:status", old_status, -1)


def handle_order_item(r, payload):
    op = payload.get("op")
    after = payload.get("after")
    if op not in ("c", "r") or not after:
        return
    product_id = after.get("product_id")
    qty = after.get("quantity")
    if product_id is None or qty is None:
        return
    bucket = f"trending:min:{event_minute(payload)}"
    r.zincrby(bucket, float(qty), str(product_id))
    r.expire(bucket, TRENDING_TTL_S)


def main():
    print("Real-time aggregator started.")
    print(f"  Kafka            : {KAFKA_BOOTSTRAP}")
    print(f"  Redis            : {REDIS_URL}")
    print(f"  Trending window  : {TRENDING_WINDOW_MIN} min")
    print(f"  Consumer group   : realtime-aggregator (no offset commit -> rebuilds on boot)")

    r = connect_redis()
    flush_owned_keys(r)
    preload_dimensions(r)

    consumer = KafkaConsumer(
        ORDERS_TOPIC,
        ORDER_ITEMS_TOPIC,
        USERS_TOPIC,
        PRODUCTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="realtime-aggregator",
        auto_offset_reset="earliest",
        # Never commit: every boot replays the whole log and rebuilds Redis, so
        # Redis is a pure derived view of Kafka and can be thrown away any time.
        enable_auto_commit=False,
        value_deserializer=deserialize,
    )
    print("Consuming live CDC stream -> Redis serving layer...")

    for message in consumer:
        payload = (message.value or {}).get("payload")
        if not payload:
            continue
        topic = message.topic
        if topic == ORDERS_TOPIC:
            handle_order(r, payload)
        elif topic == ORDER_ITEMS_TOPIC:
            handle_order_item(r, payload)
        elif topic == USERS_TOPIC:
            apply_user(r, payload)
        elif topic == PRODUCTS_TOPIC:
            apply_product(r, payload)

        # Anchor the trending window on the data's own clock, not wall-clock time.
        # The reader unions the last N minute-buckets ending at this value, so the
        # ranking stays correct under clock skew and while replaying a backlog
        # (whose events carry old timestamps) on startup.
        if topic in (ORDERS_TOPIC, ORDER_ITEMS_TOPIC):
            r.set("metrics:last_event_min", event_minute(payload))
        r.incr("metrics:events:processed")
        r.set("metrics:updated_at", datetime.now(timezone.utc).isoformat())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
