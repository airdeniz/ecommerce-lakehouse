import os
import sys
sys.stdout.reconfigure(line_buffering=True)
import time
import random
import psycopg2

DSN = os.getenv("PG_DSN", "host=localhost port=5433 dbname=ecommerce user=postgres password=postgres")

# Order lifecycle as a state machine. For each current status, the possible next
# transitions as (next_status, relative_weight, min_delay_s, max_delay_s). A
# next_status of None means "stay here" — terminal for the demo. Delays are
# compressed to seconds so a full lifecycle plays out within a couple of minutes;
# in real life these span hours to days.
#
#   CREATED -> PAID -> PREPARING -> SHIPPED -> DELIVERED        (happy path)
#   CREATED -> CANCELLED                                        (unpaid drop-off)
#   PAID / PREPARING -> REFUNDED                                (paid then reversed)
#   DELIVERED -> RETURNED -> REFUNDED                           (return flow)
LIFECYCLE = {
    "CREATED":   [("PAID", 82, 5, 20), ("CANCELLED", 18, 5, 20)],
    "PAID":      [("PREPARING", 90, 8, 25), ("REFUNDED", 6, 8, 25), (None, 4, 0, 0)],
    "PREPARING": [("SHIPPED", 94, 8, 30), ("REFUNDED", 6, 8, 30)],
    "SHIPPED":   [("DELIVERED", 96, 10, 40), (None, 4, 0, 0)],
    "DELIVERED": [(None, 90, 0, 0), ("RETURNED", 10, 15, 45)],
    "RETURNED":  [("REFUNDED", 100, 8, 25)],
    "CANCELLED": [(None, 100, 0, 0)],
    "REFUNDED":  [(None, 100, 0, 0)],
}


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

    # Status transitions are not applied inline. In real life an order sits in a
    # state for a while before it moves on, so each transition is scheduled for a
    # short delay later and applied on a subsequent loop iteration. Applying it in
    # the same iteration produced a state and its UPDATE within microseconds of
    # each other, which looked fake in the CDC stream. An order walks the LIFECYCLE
    # state machine one hop at a time: applying a hop schedules the next one.
    pending = []  # list of (due_ts, order_id, new_status)

    def schedule_next(order_id, from_status, now):
        """Pick and schedule the next lifecycle hop for an order (if any)."""
        options = LIFECYCLE.get(from_status)
        if not options:
            return
        idx = random.choices(range(len(options)), weights=[o[1] for o in options], k=1)[0]
        to_status, _weight, dmin, dmax = options[idx]
        if to_status is None:
            return  # terminal state for this order
        due = now + random.uniform(dmin, dmax)
        pending.append((due, order_id, to_status))

    def flush_pending(now):
        """Apply due transitions, then schedule each order's following hop."""
        due_now = [p for p in pending if p[0] <= now]
        pending[:] = [p for p in pending if p[0] > now]
        for _due_ts, order_id, new_status in due_now:
            cur.execute(
                "UPDATE orders SET status = %s WHERE order_id = %s",
                (new_status, order_id),
            )
            print(f"  -> Order {order_id} -> {new_status}")
            schedule_next(order_id, new_status, now)

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

        # The order starts at CREATED; schedule its first lifecycle hop. Each
        # applied hop schedules the next, so the order walks the state machine
        # (PAID -> PREPARING -> SHIPPED -> DELIVERED, with cancel/return branches)
        # over time instead of jumping straight to a final status.
        schedule_next(order_id, "CREATED", time.time())

        print(f"Order {order_id} | user {user_id} | amount {round(total,2)} | {len(items)} items")

        # Occasionally (~5%) we delete an old terminal-reversed order (CANCELLED or
        # REFUNDED). In real life such orders may be cleaned out of the OLTP after
        # a while. This lets CDC delete (op='d') events flow through the pipeline;
        # downstream they are captured as is_deleted=true (soft delete).
        if random.random() < 0.05:
            cur.execute(
                "SELECT order_id FROM orders WHERE status IN ('CANCELLED', 'REFUNDED') ORDER BY random() LIMIT 1"
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
