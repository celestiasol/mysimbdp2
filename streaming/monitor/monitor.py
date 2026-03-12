from flask import Flask, request
from pymongo import MongoClient
import requests
import time

app = Flask(__name__)


mongo = MongoClient("mongodb://mongodb:27017")
db = mongo["mysimbdp"]
metrics = db["streaming_metrics"]

# threshold
AVG_TIME_THRESHOLD = 0.2

# manager endpoint
MANAGER_URL = "http://streaming-manager:5000/alert"

@app.route("/report", methods=["POST"])
def receive_report():

    data = request.json
    metrics.insert_one(data)

    avg_time = data.get("avg_processing_time_sec", 0)

    print("Received report:", data, flush=True)

    # check threshold
    if avg_time > AVG_TIME_THRESHOLD:

        alert = {
            "tenant_id": data["tenant_id"],
            "worker_id": data["worker_id"],
            "reason": "ingestion too slow",
            "avg_time": avg_time,
            "timestamp": time.time()
        }

        print("Threshold exceeded, notifying manager...", flush=True)

        try:
            requests.post(MANAGER_URL, json=alert)
        except Exception as e:
            print("Failed to notify manager:", e)

    return {"status": "ok"}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)