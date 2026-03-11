from pymongo import MongoClient
from google.cloud import storage
import json
import time

TENANT_ID = "tenantB"
BUCKET_NAME = "mysimbdp-tenant-data"

mongo = MongoClient("mongodb://mongodb:27017")
db = mongo["mysimbdp"]
bronze = db[f"{TENANT_ID}_bronze"]
silver = db[f"{TENANT_ID}_silver"]

def upload_to_gcs(data):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{TENANT_ID}/bronze_dump.json")
    blob.upload_from_string(json.dumps(data))

def transform(record):
    record["processed_at"] = time.time()
    record["latency"] = record.get("GeneratedTokens", 0) * 0.1
    return record

if __name__ == "__main__":
    data = list(bronze.find())

    upload_to_gcs(data)

    transformed = [transform(r) for r in data]
    if transformed:
        silver.insert_many(transformed)

    print("Pipeline completed")