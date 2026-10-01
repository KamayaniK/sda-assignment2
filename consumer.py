import json
import csv
import os
from kafka import KafkaConsumer

# -----------------------------
# Kafka Configuration
# -----------------------------
TOPIC = "food-delivery-orders"
BOOTSTRAP_SERVERS = ["localhost:9092"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="a3-dashboard-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# -----------------------------
# Output file
# -----------------------------
OUTPUT_FILE = "consumed_orders.csv"

fields = [
    "order_id",
    "delivery_person_id",
    "delivery_person_age",
    "delivery_person_rating",
    "city",
    "weather_conditions",
    "road_traffic_density",
    "vehicle_condition",
    "order_type",
    "vehicle_type",
    "multiple_deliveries",
    "festival",
    "time_taken_min",
    "event_type",
    "event_timestamp"
]

# Create file with headers
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()

    event_count = 0

    print("=" * 60)
    print("Kafka Consumer Started")
    print(f"Topic: {TOPIC}")
    print("=" * 60)

    try:
        for message in consumer:
            event = message.value

            writer.writerow({
                field: event.get(field)
                for field in fields
            })

            f.flush()

            event_count += 1

            print(
                f"Event {event_count}: "
                f"{event.get('event_type')} | "
                f"Order: {event.get('order_id')} | "
                f"City: {event.get('city')} | "
                f"Traffic: {event.get('road_traffic_density')} | "
                f"Delivery Time: {event.get('time_taken_min')} min"
            )

            # Stop after consuming the 300 events currently in Kafka
            if event_count >= 300:
                break

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()

    print("=" * 60)
    print(f"Total events consumed: {event_count}")
    print(f"Output file: {OUTPUT_FILE}")
    print("=" * 60)