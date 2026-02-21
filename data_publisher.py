import os
import requests
import json
from kafka import KafkaProducer
import time
from datetime import datetime, UTC, timedelta
import sys
import io

# Ensure UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ======================
# CONFIG
# ======================
API_KEY = "e50qxUK3axpvD1lN0BOMBSkYRXldGfedIzfHCNYH"
KAFKA_BROKER = "localhost:9092"
TOPIC = "test"

if not API_KEY:
    raise ValueError("NASA_API_KEY not set in environment variables")

# ======================
# KAFKA PRODUCER
# ======================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3
)

print("🚀 Starting NASA NEO streaming (past 7 days → today)...")

try:
    # Date range: past 7 days → today (UTC)
    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=7)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    url = (
        "https://api.nasa.gov/neo/rest/v1/feed?"
        f"start_date={start_str}&"
        f"end_date={end_str}&"
        f"api_key={API_KEY}"
    )
    print(f"🌍 Fetching NASA data from {start_str} to {end_str}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Iterate and simulate real-time streaming
    for date, asteroids in data.get("near_earth_objects", {}).items():
        for asteroid in asteroids:
            producer.send(TOPIC, asteroid)

            print(
                f"📡 Sent: {asteroid.get('name')} "
                f"(ID={asteroid.get('id')}, Date={date})"
            )

            time.sleep(2)  # simulate real-time arrival

    producer.flush()
    print("✅ All asteroid events sent successfully.\n")

    # Keep service alive (optional)
    while True:
        time.sleep(60)
        print(
            f"🟢 Producer alive at "
            f"{datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

except Exception as e:
    print("❌ Error:", e)

finally:
    producer.close()