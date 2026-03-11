from kafka import KafkaProducer
import json
import csv
import sys
import time

DATASET = sys.argv[1]
TOPIC = sys.argv[2]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Starting producer for dataset {DATASET} → topic {TOPIC}")

with open(DATASET, "r") as f:
    reader = csv.DictReader(f)

    for row in reader:
        producer.send(TOPIC, row)
        print("Sent:", row)

producer.flush()
print("Dataset ingestion complete")