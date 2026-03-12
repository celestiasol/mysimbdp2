from pymongo import MongoClient
from google.cloud import storage
from datetime import datetime
import json
import time

TENANT_ID = "tenantB"
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

    context_tokens = int(record.get("ContextTokens", 0))
    generated_tokens = int(record.get("GeneratedTokens", 0))

    # convert timestamp
    try:
        ts = datetime.strptime(record["TIMESTAMP"], "%Y-%m-%d %H:%M:%S.%f")
    except:
        ts = None

    # simple derived metric
    token_ratio = generated_tokens / context_tokens if context_tokens else 0

    return {
        "timestamp": ts,
        "context_tokens": context_tokens,
        "generated_tokens": generated_tokens,
        "token_ratio": token_ratio,
        "processed_at": time.time()
    }


if __name__ == "__main__":

    data = list(bronze.find())

    if not data:
        print("No bronze data found")
        exit()

    # Convert ObjectId to string before uploading to GCS
    for d in data:
        d["_id"] = str(d["_id"])

    upload_to_gcs(data)

    transformed = [transform(r) for r in data]

    if transformed:
        silver.insert_many(transformed)

    print(f"Inserted {len(transformed)} records into tenantB_silver")