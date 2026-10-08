# databricks-retail-lakehouse-pipeline
Enterprise Medallion Lakehouse on Databricks with PySpark, Unity Catalog, Delta Lake, and Automated Workflows.

* Architected an automated 3-tier Medallion Lakehouse on Databricks using Unity Catalog volumes and managed Delta tables.
* Engineered batch PySpark pipelines to cleanse raw transactional streams, enforce schema typing, filter anomalous records, and populate Silver Delta tables.
* Aggregated multidimensional business KPIs (regional revenue, customer volume, average order margins) into an optimized Gold Delta Lake mart.
* Optimized query latency using Delta file compaction (OPTIMIZE/Z-ORDER) and orchestrated end-to-end execution pipelines using Databricks Workflows.
