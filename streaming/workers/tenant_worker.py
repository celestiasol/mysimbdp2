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
    except:
        time.sleep(5)

mongo = MongoClient(MONGO_URI)
db = mongo["mysimbdp"]
collection = db[f"{TENANT_ID}_bronze"]

print(f"DB: {db}", flush=True)
print(f"Collection: {collection}", flush=True)
print("Connected to Mongo", flush=True)

print("Partitions:", consumer.partitions_for_topic(TOPIC), flush=True)

print("Waiting for messages...", flush=True)

for message in consumer:
    start = time.time()

    print("Message received:", message.value, flush=True)

    data = message.value
    collection.insert_one(data)

    print("Inserted into MongoDB", flush=True)

    duration = time.time() - start

    report = {
        "tenant_id": TENANT_ID,
        "processing_time": duration,
        "timestamp": time.time()
    }

    try:
        requests.post(MONITOR_URL, json=report)
        print("Report sent to monitor", flush=True)
    except Exception as e:
        print("Monitor report failed:", e, flush=True)