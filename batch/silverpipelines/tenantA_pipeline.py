from pymongo import MongoClient
from google.cloud import storage
from datetime import datetime
import json
import time

TENANT_ID = "tenantA"
BUCKET_NAME = "mysimbdp-tenant-data"

mongo = MongoClient("mongodb://mongodb:27017")
db = mongo["mysimbdp"]

bronze = db[f"{TENANT_ID}_bronze"]
silver = db[f"{TENANT_ID}_silver"]


def upload_to_gcs(data):

    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob_path = f"{TENANT_ID}/caching-dir/{timestamp}.json"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(json.dumps(data))

    print(f"Uploaded bronze dump to GCS: {blob_path}")


def transform(record):

    cpu = int(record.get("cpu_request", 0))
    gpu = int(record.get("gpu_request", 0))
    memory = float(record.get("memory_request", 0))

    # Categorize workload size
    if gpu >= 4:
        workload_class = "gpu_heavy"
    elif cpu >= 16:
        workload_class = "cpu_heavy"
    else:
        workload_class = "balanced"

    return {
        "instance_sn": record.get("instance_sn"),
        "app_name": record.get("app_name"),
        "role": record.get("role"),

        "cpu_request": cpu,
        "gpu_request": gpu,
        "memory_request": memory,

        "workload_class": workload_class,

        "processed_at": time.time()
    }


if __name__ == "__main__":

    data = list(bronze.find())

    if not data:
        print("No bronze data found")
        exit()

    # Remove Mongo ObjectId before uploading to GCS
    for d in data:
        d["_id"] = str(d["_id"])

    upload_to_gcs(data)

    transformed = [transform(r) for r in data]

    if transformed:
        silver.insert_many(transformed)

    print(f"Inserted {len(transformed)} records into silver collection")