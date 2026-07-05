import os
import sys
sys.stdout.reconfigure(line_buffering=True)
import time
import random
import psycopg2

DSN = os.getenv("PG_DSN", "host=localhost port=5433 dbname=ecommerce user=postgres password=postgres")

def build_profiles(user_ids):
    """Assign each user a stable behavioural profile for this run.

    A uniform-random generator produces flat, structureless data. A light
    per-user profile gives the order stream some realistic variation instead.
    """
    profiles = {}
    for uid in user_ids:
        profiles[uid] = {
            # Pareto-distributed weight: most users are light buyers, a few are
            # very heavy. Used to bias the random user choice below.
            "weight": random.paretovariate(1.5),
            # Typical basket size for this user (drives per-user variation).
            "basket_bias": random.randint(1, 4),
        }
    return profiles


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT user_id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products")
    products = cur.fetchall()

    profiles = build_profiles(user_ids)

    # Status transitions (CREATED -> PAID/CANCELLED) are not applied inline.
    # In real life an order sits in CREATED for a while before it is paid or
    # cancelled, so we schedule the transition for at least MIN_STATUS_DELAY_S
    # seconds later and apply it on a subsequent loop iteration. Applying it in
    # the same iteration produced CREATED and its UPDATE within microseconds of
    # each other, which looked fake in the CDC stream.
    MIN_STATUS_DELAY_S = 5.0
    MAX_STATUS_DELAY_S = 20.0
    pending = []  # list of (due_ts, order_id, new_status)

    def flush_pending(now):
        """Apply any scheduled status transitions whose delay has elapsed."""
        still_pending = []
        for due_ts, order_id, new_status in pending:
            if due_ts <= now:
                cur.execute(
                    "UPDATE orders SET status = %s WHERE order_id = %s",
                    (new_status, order_id),
                )
                print(f"  -> Order {order_id} -> {new_status}")
            else:
                still_pending.append((due_ts, order_id, new_status))
        pending[:] = still_pending

    print(f"Order generator started ({len(user_ids)} users, {len(products)} products). "
          "Press Ctrl+C to stop.")
    while True:
        # Apply due status transitions from earlier iterations first.
        flush_pending(time.time())

        # Bias user choice by their Pareto weight: a few heavy buyers, many light.
        weights = [profiles[u]["weight"] for u in user_ids]
        user_id = random.choices(user_ids, weights=weights, k=1)[0]
        prof = profiles[user_id]

        # Basket size centred on the user's bias, clamped to a sane range.
        k = int(round(random.gauss(prof["basket_bias"], 1)))
        k = max(1, min(len(products), k))
        qty_max = 4

        chosen = random.sample(products, k=k)

        total = 0
        items = []
        for product_id, price in chosen:
            qty = random.randint(1, qty_max)
            total += float(price) * qty
            items.append((product_id, qty, float(price)))

        cur.execute(
            "INSERT INTO orders (user_id, status, total_amount) VALUES (%s, 'CREATED', %s) RETURNING order_id",
            (user_id, round(total, 2)),
        )
        order_id = cur.fetchone()[0]

        for product_id, qty, price in items:
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                (order_id, product_id, qty, price),
            )
            cur.execute(
                "UPDATE inventory SET stock_qty = stock_qty - %s WHERE product_id = %s",
                (qty, product_id),
            )

        # Decide the order's eventual fate now, but defer the actual UPDATE so
        # a realistic gap sits between the CREATED insert and its transition.
        roll = random.random()
        if roll < 0.70:
            due = time.time() + random.uniform(MIN_STATUS_DELAY_S, MAX_STATUS_DELAY_S)
            pending.append((due, order_id, "PAID"))
        elif roll < 0.85:
            due = time.time() + random.uniform(MIN_STATUS_DELAY_S, MAX_STATUS_DELAY_S)
            pending.append((due, order_id, "CANCELLED"))

        print(f"Order {order_id} | user {user_id} | amount {round(total,2)} | {len(items)} items")

        # Occasionally (~5%) we delete an old CANCELLED order. In real life
        # cancelled orders may be cleaned out of the OLTP after a while.
        # This lets CDC delete (op='d') events flow through the pipeline;
        # downstream they are captured as is_deleted=true (soft delete).
        if random.random() < 0.05:
            cur.execute(
                "SELECT order_id FROM orders WHERE status = 'CANCELLED' ORDER BY random() LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                to_delete = row[0]
                cur.execute("DELETE FROM order_items WHERE order_id = %s", (to_delete,))
                cur.execute("DELETE FROM orders WHERE order_id = %s", (to_delete,))
                print(f"  -> Order {to_delete} deleted (cancellation cleanup)")

        time.sleep(random.uniform(2.0, 5.0))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
