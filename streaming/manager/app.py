import yaml
import subprocess
import os
import time
from flask import Flask, request
import threading

CONFIG_PATH = "/app/config/tenants"

app = Flask(__name__)

def load_tenants():
    tenants = []
    for file in os.listdir(CONFIG_PATH):
        if file.endswith(".yaml"):
            with open(os.path.join(CONFIG_PATH, file)) as f:
                tenants.append(yaml.safe_load(f))
    return tenants

def start_worker(tenant):
    print(f"Starting worker for {tenant['tenant_id']}", flush=True)

    subprocess.Popen([
        "python",
        f"/app/workers/{tenant['worker_script']}",
        tenant["tenant_id"],
        tenant["streaming"]["topic"]
    ])

@app.route("/alert", methods=["POST"])
def receive_alert():
    data = request.json
    print("\n===== ALERT RECEIVED =====", flush=True)
    print(data, flush=True)
    print("==========================", flush=True)

    return {"status": "received"}

def start_workers():
    tenants = load_tenants()
    for tenant in tenants:
        start_worker(tenant)

if __name__ == "__main__":

    # start workers in background
    threading.Thread(target=start_workers).start()

    # start API
    app.run(host="0.0.0.0", port=5000)