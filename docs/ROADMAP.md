# Roadmap

## Implemented in this repository

- [x] Reproducible synthetic transaction generator
- [x] Offline Bronze/Silver/Gold ETL with quarantined invalid/duplicate records
- [x] Exact Decimal daily monetary aggregation and reconciliation
- [x] Transparent review-only risk rules
- [x] Automated Python tests and GitHub Actions workflow
- [x] Optional unsupervised ML anomaly experiment
- [x] Databricks/PySpark/Delta reference job (requires external workspace)

## Future improvements, not yet implemented

- [ ] Azure Data Lake Storage Gen2 and workspace provisioning
- [ ] Unity Catalog grants, credential management, encryption and auditing
- [ ] Event Hubs/Kafka ingestion, micro-batching and CDC
- [ ] Immutable Bronze ingest metadata, watermark and late-arrival handling
- [ ] Databricks Workflows and parameterized environment deployments
- [ ] Full source-to-target reconciliation, streaming observability and alert routing
- [ ] Production-grade ML evaluation, human-in-the-loop checks and drift monitoring
- [ ] Infrastructure-as-code, integration smoke tests and failure recovery

No operational Azure resources are provisioned or deployed by this code.
