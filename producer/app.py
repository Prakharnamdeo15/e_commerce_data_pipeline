

import json
import uuid
import random
import threading
import time
from datetime import datetime, timedelta
from fastapi import FastAPI
from confluent_kafka import Producer

app = FastAPI()

KAFKA_BROKER = "kafka:9092"
TOPIC = "order_events"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "linger.ms": 5,
    "acks": "all"
})

# In-memory order version tracking
order_versions = {}

# Track active orders for realistic lifecycle
active_orders = []
categories = ["electronics","fashion","home"]
payment_type = ["UPI","CARD","COD"]
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

def generate_order_event():
    global order_versions, active_orders

    # 70% chance create new order
    if not active_orders or random.random() < 0.7:
        order_id = str(uuid.uuid4())
        version = 1
        order_versions[order_id] = version
        active_orders.append(order_id)
        event_type = "CREATED"
        amount = round(random.uniform(10, 500), 2)

    else:
        order_id = random.choice(active_orders)
        version = order_versions[order_id] + 1
        order_versions[order_id] = version

        event_type = random.choices(
            ["UPDATED", "REFUNDED"],
            weights=[0.7, 0.3]
        )[0]

        amount = round(random.uniform(5, 200), 2)

        if event_type == "REFUNDED":
            active_orders.remove(order_id)

    # Simulate late data (0–30 minutes)
    late_offset = random.randint(0, 30)
    event_time = datetime.utcnow() - timedelta(minutes=late_offset)

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"U{random.randint(1000,5000)}",
        "order_id": order_id,
        "event_type": event_type,
        "version": version,
        "amount": amount,
        "status": event_type,
        "event_time": event_time.isoformat(),
        "category": random.choice(categories),
        "payment_type": random.choice(payment_type)
    }

    # Occasionally produce duplicate
    if random.random() < 0.05:
        producer.produce(
            TOPIC,
            key=order_id,
            value=json.dumps(event),
            on_delivery=delivery_report
        )

    producer.produce(
        TOPIC,
        key=order_id,
        value=json.dumps(event),
        on_delivery=delivery_report
    )
    print(event)

    producer.poll(0)

def background_generator(rate_per_sec=50):
    while True:
        start = time.time()

        for _ in range(rate_per_sec):
            generate_order_event()

        producer.flush()

        elapsed = time.time() - start
        sleep_time = max(0, 1 - elapsed)
        time.sleep(sleep_time)

@app.on_event("startup")
def start_background_thread():
    thread = threading.Thread(
        target=background_generator,
        args=(100,),  # events per second
        daemon=True
    )
    thread.start()

@app.post("/burst")
def burst_events(count: int = 1000):
    for _ in range(count):
        generate_order_event()

    producer.flush()
    return {"status": f"{count} events produced"}

@app.get("/health")
def health():
    return {"status": "producer running "}