from flask import Flask, request
from pymongo import MongoClient
import time

app = Flask(__name__)

mongo = MongoClient("mongodb://mongodb:27017")
db = mongo["mysimbdp"]
metrics = db["streaming_metrics"]

@app.route("/report", methods=["POST"])
def report():
    data = request.json
    metrics.insert_one(data)

    # simple SLA check
    if data["processing_time"] > 2:
        print(f"WARNING: Slow processing for {data['tenant_id']}")

    return {"status": "ok"}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)