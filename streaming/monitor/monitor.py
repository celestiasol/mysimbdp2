from flask import Flask, request
from pymongo import MongoClient
import requests
import time

app = Flask(__name__)

# MongoDB for metrics storage
mongo = MongoClient("mongodb://mongodb:27017")
db = mongo["mysimbdp"]
metrics_collection = db["streaming_metrics"]

# Configurable thresholds
AVG_TIME_THRESHOLD = 0.05  # seconds per record

# Streaming Manager API endpoint
MANAGER_URL = "http://streaming-manager:5000/alert"

@app.route("/report", methods=["POST"])
def receive_report():
    data = request.get_json()
    # Store metrics in MongoDB
    metrics_collection.insert_one({
        "tenant_id": data["tenant_id"],
        "worker_id": data["worker_id"],
        "avg_processing_time_sec": data.get("avg_processing_time_sec"),
        "records_processed": data.get("records_processed"),
        "timestamp": time.time()
    })

    # Check if metrics exceed threshold
    if data.get("avg_processing_time_sec", 0) > AVG_TIME_THRESHOLD:
        alert = {
            "tenant_id": data["tenant_id"],
            "worker_id": data["worker_id"],
            "reason": "slow ingestion",
            "avg_time": data.get("avg_processing_time_sec")
        }
        try:
            requests.post(MANAGER_URL, json=alert)
        except Exception as e:
            print(f"Error alerting manager: {e}")

    return {"status": "ok"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)