# Architecture

```

                   Kafka Messaging
                          │
            ┌─────────────┴─────────────┐
            │                           │
     tenantA_worker              tenantB_worker
            │                           │
            ▼                           ▼
     tenantA_bronze              tenantB_bronze
            │                           │
            └─────────────┬─────────────┘
                          ▼
                    Batch Manager
                          │
            ┌─────────────┴─────────────┐
            │                           │
     tenantA_pipeline            tenantB_pipeline
                          │
                          ▼
                    MongoDB Silver

```

