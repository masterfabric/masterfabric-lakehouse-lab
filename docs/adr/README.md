# Architecture Decision Records

Each ADR records one decision: the context, the options that were on the table, what was
chosen, and what that choice costs. Format: lightweight [MADR](https://adr.github.io/madr/).

| # | Decision | Status |
|---|---|---|
| [0001](0001-table-format-iceberg.md) | Apache Iceberg as the table format | Accepted |
| [0002](0002-object-storage.md) | S3-compatible object storage, RustFS in the lab | Accepted |
| [0003](0003-rest-catalog.md) | Iceberg REST catalog instead of a Hive Metastore | Accepted |
| [0004](0004-medallion-contracts.md) | Medallion layers with explicit contracts and quarantine | Accepted |
| [0005](0005-gold-marts.md) | Gold marts rebuilt by atomic overwrite; panel weight; baseline churn score | Accepted |
| [0006](0006-engines.md) | Spark for pipelines, Trino for interactive SQL | Accepted |
| [0007](0007-table-maintenance.md) | Merge-on-read writes with scheduled compaction and snapshot expiry | Accepted |
