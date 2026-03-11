import yaml
import subprocess
import os
import time

CONFIG_PATH = "/app/config/tenants"

def load_tenants():
    tenants = []
    for file in os.listdir(CONFIG_PATH):
        if file.endswith(".yaml"):
            with open(os.path.join(CONFIG_PATH, file)) as f:
                tenants.append(yaml.safe_load(f))
    return tenants

def start_worker(tenant):
    print(f"Starting worker for {tenant['tenant_id']}")
    subprocess.Popen(
        ["python", f"/app/workers/{tenant['worker_script']}", f"{tenant['tenant_id']}", f"{tenant['streaming']['topic']}"]
    )

if __name__ == "__main__":
    tenants = load_tenants()
    for tenant in tenants:
        start_worker(tenant)

    # keep container alive
    while True:
        time.sleep(60)