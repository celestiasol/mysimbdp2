# import yaml
# import os
# import subprocess
# import time
# from pymongo import MongoClient
# from croniter import croniter
# from datetime import datetime


# CONFIG_PATH = "/app/config/tenants"
# LOG_COLLECTION = "batch_logs"

# mongo = MongoClient("mongodb://mongodb:27017")
# db = mongo["mysimbdp"]

# def load_tenants():
#     tenants = []
#     for file in os.listdir(CONFIG_PATH):
#         if file.endswith(".yaml"):
#             with open(os.path.join(CONFIG_PATH, file)) as f:
#                 tenants.append(yaml.safe_load(f))
#     return tenants

# def check_constraints(tenant):
#     # simple constraint: max executions per hour
#     logs = db[LOG_COLLECTION]
#     last_hour = time.time() - 3600
#     count = logs.count_documents({
#         "tenant_id": tenant["tenant_id"],
#         "timestamp": {"$gt": last_hour}
#     })
#     return count < tenant["sla"]["max_runs_per_hour"]

# def should_run(schedule):
#     base = datetime.now()
#     itr = croniter(schedule, base)
#     prev = itr.get_prev(datetime)
#     return (base - prev).seconds < 60

# def run_pipeline(tenant):
#     start = time.time()

#     result = subprocess.run(
#         ["python", f"/app/silverpipelines/{tenant['pipeline_script']}"],
#         capture_output=True
#     )

#     duration = time.time() - start

#     db[LOG_COLLECTION].insert_one({
#         "tenant_id": tenant["tenant_id"],
#         "duration": duration,
#         "success": result.returncode == 0,
#         "timestamp": time.time()
#     })

# if __name__ == "__main__":
#     tenants = load_tenants()

#     for tenant in tenants:
#         scheduled = should_run(tenant["batch"]["schedule"])
#         allowed = check_constraints(tenant)

#         if scheduled and allowed:
#             run_pipeline(tenant)

#         elif not scheduled:
#             print(f"Not scheduled now: {tenant['tenant_id']}")

#         elif not allowed:
#             print(f"SLA violation: skipping {tenant['tenant_id']}")

import yaml
import os
import subprocess
import time
from pymongo import MongoClient
from croniter import croniter
from datetime import datetime

CONFIG_PATH = "/app/config/tenants"
LOG_COLLECTION = "batch_logs"

mongo = MongoClient("mongodb://mongodb:27017")
db = mongo["mysimbdp"]

def load_tenants():
    tenants = []
    for file in os.listdir(CONFIG_PATH):
        if file.endswith(".yaml"):
            with open(os.path.join(CONFIG_PATH, file)) as f:
                tenants.append(yaml.safe_load(f))
    return tenants

def check_constraints(tenant):
    logs = db[LOG_COLLECTION]
    last_hour = time.time() - 3600

    count = logs.count_documents({
        "tenant_id": tenant["tenant_id"],
        "timestamp": {"$gt": last_hour}
    })

    return count < tenant["sla"]["max_runs_per_hour"]

def should_run(schedule):
    base = datetime.now()
    itr = croniter(schedule, base)
    prev = itr.get_prev(datetime)

    return (base - prev).total_seconds() < 70

def run_pipeline(tenant):
    print(f"Running pipeline for {tenant['tenant_id']}", flush=True)

    start = time.time()

    result = subprocess.run(
        ["/usr/local/bin/python", f"/app/silverpipelines/{tenant['pipeline_script']}"],
        capture_output=True,
        text=True
    )

    duration = time.time() - start

    print(result.stdout)
    print(result.stderr)

    db[LOG_COLLECTION].insert_one({
        "tenant_id": tenant["tenant_id"],
        "duration": duration,
        "success": result.returncode == 0,
        "timestamp": time.time()
    })


def scheduler_loop():
    print("Batch manager started", flush=True)

    while True:
        try:
            tenants = load_tenants()

            for tenant in tenants:
                scheduled = should_run(tenant["batch"]["schedule"])
                allowed = check_constraints(tenant)

                if scheduled and allowed:
                    run_pipeline(tenant)

                elif not scheduled:
                    print(f"Not scheduled now: {tenant['tenant_id']}")

                elif not allowed:
                    print(f"SLA violation: skipping {tenant['tenant_id']}")

        except Exception as e:
            print("Scheduler error:", e, flush=True)

        time.sleep(60)


if __name__ == "__main__":
    scheduler_loop()