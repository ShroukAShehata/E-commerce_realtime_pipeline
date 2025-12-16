from kafka import KafkaProducer
import json, time, random
from datetime import datetime, UTC

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    api_version_auto_timeout_ms=30000,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_event():
    return {
        "user_id": random.randint(1,1000),
        "session_id": f"sess_{random.randint(1,300)}",
        "event_type": random.choice(["page_view","add_to_cart","purchase"]),
        "product_id": random.randint(1,200),
        "ts": datetime.now(UTC).isoformat()
    }

while True:
    ev = generate_event()
    producer.send("clickstream_raw", ev)
    producer.flush()
    print("sent", ev)
    time.sleep(0.2)
