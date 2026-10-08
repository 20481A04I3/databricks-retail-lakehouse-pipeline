# databricks-retail-lakehouse-pipeline
Enterprise Medallion Lakehouse on Databricks with PySpark, Unity Catalog, Delta Lake, and Automated Workflows.


# Enterprise Retail Lakehouse - Azure Databricks & Unity Catalog

An end-to-end Medallion Lakehouse pipeline engineered on **Databricks** using **Unity Catalog**, **PySpark**, **Delta Lake**, and **Databricks Workflows** for automated batch orchestration and business aggregation.

---

## Architecture Overview

```text
[Raw Retail Transactions CSV]
       ↓ (Ingestion into Unity Catalog Volume)
[Bronze Layer: Managed Delta Table (bronze_transactions)]
       ↓ (PySpark Notebook Transformation & Anomaly Filtering)
[Silver Layer: Cleaned Delta Table (silver_clean_transactions)]
       ↓ (Multidimensional Aggregations & Delta Optimization)
[Gold Layer: Business Mart (gold_country_sales_metrics)]
       ↓ (Automated Orchestration)
[Databricks Workflows (Jobs) Scheduled Execution]

Key Highlights & Pipeline Stages
Bronze Layer (Ingestion & Lineage):

Staged raw e-commerce retail transaction files into modern Unity Catalog Volumes (/Volumes/workspace/retail_lakehouse/bronze_volume/).

Built a managed Delta Lake table (bronze_transactions) with append-only ingestion audit timestamps (ingestion_timestamp).

Silver Layer (PySpark Cleansing & Schema Enforcement):

Filtered out transaction anomalies: cancelled orders (invoice numbers prefixed with 'C'), zero or negative quantities, and sub-zero unit prices.

Standardized table schema to lowercase snake_case.

Handled missing customer references using F.coalesce default imputation.

Derived transactional line-item pricing (total_amount = round(quantity * unit_price, 2)) and persisted to ACID-compliant Delta table silver_clean_transactions.

Gold Layer (Business Aggregations & Serving Mart):

Aggregated 3,000+ line items into country-level dimensional KPIs: total_revenue, total_orders, unique_customers, total_units_sold, and avg_order_value.

Persisted aggregated metrics to managed Delta table gold_country_sales_metrics.

Performance Tuning & Storage Optimization:

Compacted Delta Parquet files using the OPTIMIZE command to mitigate small-file problems and accelerate downstream analytical query performance.

Production Orchestration (Databricks Workflows):

Configured an automated orchestration job (run_medallion_pipeline) triggered via Databricks Workflows / Jobs.

Verified automated run execution completing end-to-end ingestion and transformation with Status: Succeeded.

Repository Contents
01_bronze_to_silver_databricks.py: Complete PySpark extraction, cleaning, and Gold aggregation script.

Screenshots/: Pipeline execution runs, Databricks Workflow job logs, and Gold table outputs.
