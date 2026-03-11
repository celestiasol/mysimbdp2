db = db.getSiblingDB("mysimbdp")

db.createCollection("tenantA_bronze")
db.createCollection("tenantA_silver")
db.createCollection("tenantB_bronze")
db.createCollection("tenantB_silver")
db.createCollection("batch_logs")
db.createCollection("ingestion_metrics")

db.tenantA_bronze.createIndex({ tenantId: 1 })
db.tenantA_silver.createIndex({ tenantId: 1 })
db.tenantB_bronze.createIndex({ tenantId: 1 })
db.tenantB_silver.createIndex({ tenantId: 1 })
