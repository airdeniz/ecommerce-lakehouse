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

    print(f"Order generator started ({len(user_ids)} users, {len(products)} products). "
          "Press Ctrl+C to stop.")
    while True:
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

        roll = random.random()
        if roll < 0.70:
            cur.execute("UPDATE orders SET status = 'PAID' WHERE order_id = %s", (order_id,))
        elif roll < 0.85:
            cur.execute("UPDATE orders SET status = 'CANCELLED' WHERE order_id = %s", (order_id,))

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
