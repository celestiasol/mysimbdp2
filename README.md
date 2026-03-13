# mysimbdp – Multi-Tenant Streaming & Batch Data Platform

mysimbdp is a multi-tenant data ingestion and processing platform designed to support near real-time streaming ingestion and scheduled batch transformations. The platform allows multiple tenants to send data through a messaging system, ingest it as bronze data, transform it into silver data, and store the results for further analysis.

The system demonstrates a data lake style architecture (Bronze → Silver) using containerized services.

⸻

# Architecture Overview

The platform consists of the following main components:

## Messaging System
	•	Kafka acts as the messaging system.
	•	Tenants send data through Kafka topics.

## Streaming Ingestion Layer
	•	Streaming Workers consume Kafka messages.
	•	Each tenant has its own worker logic.
	•	Workers insert records into MongoDB Bronze collections.

## Monitoring Layer
	•	Workers send ingestion performance metrics to Streaming Monitor.
	•	The monitor evaluates performance thresholds.

## Management Layer
	•	Streaming Ingest Manager controls worker lifecycle.
	•	It can start or stop workers dynamically.

## Storage Layer
	•	MongoDB (mysimbdp-coredms) stores:
	•	Bronze data
	•	Silver data
	•	Batch logs

## Batch Processing Layer
	•	Batch Manager
	•	Executes tenant silver transformation pipelines
	•	Enforces service level agreement (SLA) constraints

## Tenant Caching Directory
	•	Implemented using Google Cloud Storage
	•	Temporary storage for extracted bronze data before transformation.

⸻

# Project Structure

```
mysimbdp/
│
├── docker-compose.yml
├── README.md
│
├── coredms/
│   └── init.js
│
├── streaming/
│   ├── manager/
│   ├── monitor/
│   └── workers/
│       ├── tenantA_worker.py
│       └── tenantB_worker.py
│
├── batch/
│   ├── batchmanager.py
│
├── silverpipelines/
│   ├── tenantA_pipeline.py
│   └── tenantB_pipeline.py
│
├── config/
│   └── tenants/
│       ├── tenantA.yaml
│       └── tenantB.yaml
│
└── producers/
    └── producer.py
```

⸻

# Technologies Used

Messaging System	       Apache Kafka
Database	              MongoDB
Containerization	       Docker & Docker Compose
Cloud Storage	              Google Cloud Storage
Scheduling	              Python croniter
Programming Language	       Python
Configuration	              YAML


⸻

# Setup Instructions

## 1. Clone the Repository

git clone [<repo-url>](https://github.com/celestiasol/mysimbdp2)
cd mysimbdp2


⸻

## 2. Configure Google Cloud Storage Credentials

Create a service account key and place it in:

gcp/credentials/gcp-storage-key.json

Then ensure the container environment variable is set:

GOOGLE_APPLICATION_CREDENTIALS=/app/gcp-storage-key.json


⸻

## 3. Start the Platform

Run:

docker-compose up --build

This will start:
	•	MongoDB
	•	Zookeeper
	•	Kafka
	•	Streaming Manager
	•	Streaming Monitor
	•	Streaming Workers
	•	Batch Manager

⸻

# Sending Test Data

Use the Kafka console producer:
```
kafka-console-producer \
--broker-list localhost:9092 \
--topic tenantA-bronze
```
Then send JSON records.

Example:
```
{"instance_sn":"instance_1","cpu_request":"12","gpu_request":"1","memory_request":"120.0"}
```

⸻

# Streaming Data Flow
	1.	Tenant producer sends messages to Kafka
	2.	Streaming workers consume Kafka topics
	3.	Data is inserted into MongoDB Bronze collections
	4.	Workers report performance metrics to Streaming Monitor
	5.	Monitor can notify Streaming Manager when performance drops

⸻

# Batch Transformation (Silver Pipelines)

The Batch Manager periodically executes tenant pipelines.

Process:
	1.	Read tenant configuration from YAML
	2.	Check SLA constraints
	3.	Trigger tenant silver pipeline
	4.	Extract bronze data
	5.	Store raw dump in GCS tenant caching directory
	6.	Transform records
	7.	Insert results into MongoDB Silver collections

⸻

# Service Level Agreements (SLA)

Each tenant defines constraints in YAML.

Example:
```
sla:
  max_runs_per_hour: 12
  max_execution_time_seconds: 120
  priority: high
  max_storage_mb: 5000
```
The batch manager enforces these constraints before executing pipelines.

⸻

# Logging and Observability

Streaming metrics are logged in MongoDB: `streaming_metrics` collection

Each log contains:
	•	`tenant_id`: identifies which tenant the metrics belong to.
	•	`worker_id`: unique ID of the streaming worker sending the metrics.
	•	`avg_processing_time_sec`: average time taken to process a single message or batch.
	•	`records_processed`: number of messages/records processed during the reporting interval.
	•	`total_data_size_mb`: cumulative size of the data processed in MB.
	•	`timestamp`: when the metrics were recorded.

Example log:
```
{
  "tenant_id": "tenantA",
  "worker_id": "worker-tenant-a",
  "avg_processing_time_sec": 0.15,
  "records_processed": 120,
  "total_data_size_mb": 1.2,
  "timestamp": 1700000000
}
```

Usage for the logs:
       •	Track worker performance (e.g., if average processing time spikes).
	•	Detect SLA violations per tenant (e.g., exceeding allowed message latency).
	•	Calculate failure rates or processing gaps if records_processed drops unexpectedly.
	•	Measure system utilization across multiple tenants and workers.

Pipeline executions are also logged in MongoDB: `batch_logs` collection

Each log contains:
	•	tenant_id
	•	execution duration
	•	success/failure status
	•	timestamp

Example log:
```
{
  tenant_id: "tenantA",
  duration: 3.2,
  success: true,
  timestamp: 1700000000
}
```
These logs allow the platform to analyze:
	•	pipeline performance
	•	SLA violations
	•	failure rates
	•	system utilization

⸻

# Tenants Implemented

Two tenants are implemented for testing:

## Tenant A

Workload resource dataset

Transformations include:
	•	CPU/GPU extraction
	•	workload classification

Dataset used: https://github.com/alibaba/clusterdata/blob/master/cluster-trace-gpu-v2025/disaggregated_DLRM_trace.csv

## Tenant B

LLM token usage dataset

Transformations include:
	•	timestamp parsing
	•	token ratio calculation

Dataset used: https://github.com/Azure/AzurePublicDataset/blob/master/data/AzureLLMInferenceTrace_code.csv

⸻

# Running Silver Pipelines Manually

You can run pipelines manually:
```
docker exec -it mysimbdp-batch-manager python /app/silverpipelines/tenantA_pipeline.py
```

⸻

# Testing SLA Violations

Example test:

Run the pipeline repeatedly within one hour.

The batch manager will output:
```
SLA violation: skipping tenantA
```
This confirms constraint enforcement.

⸻

# Example Queries

View bronze data:
```
db.tenantA_bronze.find().limit(5)
```
View silver data:
```
db.tenantA_silver.find().limit(5)
```
View batch logs:
```
db.batch_logs.find()
```

⸻

# Summary

mysimbdp demonstrates a multi-tenant streaming and batch data processing platform with:
	•	Kafka-based ingestion
	•	MongoDB data storage
	•	Tenant-specific transformation pipelines
	•	SLA constraint enforcement
	•	Observability through logging
	•	Cloud-based caching directory

The system supports scalable, isolated tenant workloads while maintaining centralized platform control.
