"""
Stock Monitoring Service
================================================

This service is a Kafka CONSUMER. It does not touch the existing pipeline in any
way; it requires no change to Postgres, Debezium, Kafka or PySpark. It simply
connects to the already-flowing `ecom.public.inventory` topic with a new
consumer group.

IMPORTANT CONCEPTUAL DISTINCTION:
---------------------------------
Checking and DECREMENTING stock is the job of the application (OLTP) side:
  - The customer clicks "Place Order"
  - The backend checks whether stock is sufficient (stock_qty >= requested?)
  - If sufficient, it creates the order + decrements stock (UPDATE inventory ...)
  - If not, it returns an "out of stock" error
This all happens at transaction-time, within milliseconds. CDC is unaware of it.

This service does NOT manage stock; it OBSERVES stock changes. The decision has
already been made and the stock has already been decremented. This service
answers "someone did something with stock — who needs to know about it?":

  1. ALERTING / MONITORING — if stock drops below a critical threshold, notify
     the purchasing team (so they can reorder from the supplier). The app does
     not do this; its job is taking orders, not supply planning.

  2. ANALYTICS — burn-rate analysis: how many units of a product sell per day,
     when will it run out? This is not in the historical OLTP (which only has
     current stock); it is in the CDC event stream.

  3. SYNCHRONIZATION — pushing stock changes to other systems like a marketplace
     integration, a warehouse management system, or a supplier portal. Instead
     of each connecting to the OLTP separately, they read from this topic.

This simple example demonstrates use case (1): a low-stock alert.
"""

import os
import sys
import json

sys.stdout.reconfigure(line_buffering=True)

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INVENTORY_TOPIC = os.environ.get("INVENTORY_TOPIC", "ecom.public.inventory")
# An alert is produced when stock drops below this threshold
LOW_STOCK_THRESHOLD = int(os.environ.get("LOW_STOCK_THRESHOLD", "10"))
# earliest = on restart, also reads the stock changes it missed while down
# latest   = reads only new events that arrive while the service is connected
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")

# Simple memory to avoid emitting the same alert repeatedly for one product.
# (In prod this lives in Redis/DB; here in-memory is enough.)
already_alerted = set()


def handle_inventory_event(payload):
    """Extract the stock change from the Debezium envelope and evaluate it."""
    after = payload.get("after")
    if after is None:
        # Delete (op=d) — the product left inventory; we do not care
        return

    product_id = after.get("product_id")
    stock_qty = after.get("stock_qty")

    if product_id is None or stock_qty is None:
        return

    if stock_qty < LOW_STOCK_THRESHOLD:
        if product_id not in already_alerted:
            # In real life a Slack webhook / email / PagerDuty would be called here:
            #   requests.post(SLACK_WEBHOOK, json={"text": ...})
            print(
                f"[ALERT] Stock critically low! "
                f"product_id={product_id} stock_qty={stock_qty} "
                f"(threshold={LOW_STOCK_THRESHOLD}) -> notify purchasing team"
            )
            already_alerted.add(product_id)
    else:
        # Stock back to normal (restocked) -> clear the alert memory
        already_alerted.discard(product_id)


def main():
    print(f"Stock monitoring service started.")
    print(f"  Topic            : {INVENTORY_TOPIC}")
    print(f"  Low-stock threshold : {LOW_STOCK_THRESHOLD}")
    print(f"  Consumer group   : stock-monitor-service")

    consumer = KafkaConsumer(
        INVENTORY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # PySpark'tan BAGIMSIZ consumer group -> ayni topic'i bagimsiz okuruz
        group_id="stock-monitor-service",
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    )

    for message in consumer:
        if message.value is None:
            continue
        payload = message.value.get("payload")
        if payload is None:
            continue
        handle_inventory_event(payload)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDurduruldu.")
