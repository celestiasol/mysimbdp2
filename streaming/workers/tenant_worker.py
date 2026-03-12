from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import requests
import time
import sys

TENANT_ID = sys.argv[1]
TOPIC = sys.argv[2]
MONGO_URI = "mongodb://mongodb:27017"
MONITOR_URL = "http://streaming-monitor:5000/report"

print(f"Tenant ID: {TENANT_ID}", flush=True)
print(f"Topic: {TOPIC}", flush=True)

# Retry Kafka connection until available
while True:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers="kafka:29092",
            group_id=f"{TENANT_ID}_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest"
        )
        print("Connected to Kafka", flush=True)
        break
    except Exception as e:
        print("Kafka not ready, retrying in 5s...", e, flush=True)
        time.sleep(5)

# MongoDB connection
mongo = MongoClient(MONGO_URI)
db = mongo["mysimbdp"]
collection = db[f"{TENANT_ID}_bronze"]

print(f"Connected to MongoDB collection: {collection}", flush=True)
print("Partitions:", consumer.partitions_for_topic(TOPIC), flush=True)
print("Waiting for messages...", flush=True)

# Metrics accumulation
total_messages = 0
total_time = 0.0

for message in consumer:
    start = time.time()

    data = message.value
    collection.insert_one(data)

    duration = time.time() - start
    total_time += duration
    total_messages += 1
    avg_time = total_time / total_messages

    # Build report
    report = {
        "tenant_id": TENANT_ID,
        "worker_id": f"worker-{TENANT_ID}",
        "avg_processing_time_sec": avg_time,
        "records_processed": total_messages,
        "total_processing_time_sec": total_time
    }

    try:
        requests.post(MONITOR_URL, json=report)
        print(f"Report sent: {report}", flush=True)
    except Exception as e:
        print("Monitor report failed:", e, flush=True)

    print(f"Processed message {total_messages} in {duration:.4f}s (avg {avg_time:.4f}s)", flush=True)