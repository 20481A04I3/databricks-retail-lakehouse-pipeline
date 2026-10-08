# Databricks notebook source
# 1. Ensure our Unity Catalog schema & managed volume exist
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.retail_lakehouse")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.retail_lakehouse.bronze_volume")

# 2. Download the dataset locally into driver node
import urllib.request
import shutil

data_url = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/by-day/2010-12-01.csv"
local_file = "/tmp/raw_retail.csv"
urllib.request.urlretrieve(data_url, local_file)

# 3. Copy directly into Unity Catalog Volume
volume_target_path = "/Volumes/workspace/retail_lakehouse/bronze_volume/raw_retail.csv"
shutil.copyfile(local_file, volume_target_path)

print("Raw bronze dataset successfully staged in Unity Catalog Volume!")

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read raw CSV file from our Unity Catalog Volume
raw_file_path = "/Volumes/workspace/retail_lakehouse/bronze_volume/raw_retail.csv"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_file_path)

# 2. Add an ingestion timestamp column (standard Bronze practice for data lineage and auditing)
df_bronze = df_raw.withColumn("ingestion_timestamp", F.current_timestamp())

# 3. Persist into a Delta Lake table in Unity Catalog
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.retail_lakehouse.bronze_transactions")

print(f"Bronze Delta table created! Total raw rows: {df_bronze.count()}")
display(df_bronze.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# 1. Read from Bronze Delta Table
df_bronze_source = spark.table("workspace.retail_lakehouse.bronze_transactions")

# 2. Clean, standardize column names (snake_case), and eliminate anomalies:
# - Filter out cancelled orders (InvoiceNo starting with 'C')
# - Filter out negative or zero Quantity and UnitPrice
# - Handle missing CustomerID
# - Calculate total line amount
df_silver = df_bronze_source \
    .withColumnRenamed("InvoiceNo", "invoice_no") \
    .withColumnRenamed("StockCode", "stock_code") \
    .withColumnRenamed("Description", "description") \
    .withColumnRenamed("Quantity", "quantity") \
    .withColumnRenamed("InvoiceDate", "invoice_date") \
    .withColumnRenamed("UnitPrice", "unit_price") \
    .withColumnRenamed("CustomerID", "customer_id") \
    .withColumnRenamed("Country", "country") \
    .filter(~F.col("invoice_no").startswith("C")) \
    .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0)) \
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)) \
    .withColumn("customer_id", F.coalesce(F.col("customer_id").cast(IntegerType()), F.lit(-1))) \
    .withColumn("processed_timestamp", F.current_timestamp())

# 3. Write to Silver Delta Lake Table
df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.retail_lakehouse.silver_clean_transactions")

print(f"Silver Delta table successfully created! Cleaned rows: {df_silver.count()}")
display(df_silver.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read from Silver Delta Table
df_silver_source = spark.table("workspace.retail_lakehouse.silver_clean_transactions")

# 2. Compute Gold-level Aggregations: Country & Product sales performance
df_gold_country_metrics = df_silver_source.groupBy("country").agg(
    F.countDistinct("invoice_no").alias("total_orders"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.sum("quantity").alias("total_units_sold"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("avg_order_value")
).orderBy(F.col("total_revenue").desc())

# 3. Save as a managed Gold Delta table in Unity Catalog
df_gold_country_metrics.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.retail_lakehouse.gold_country_sales_metrics")

print(f"Gold Delta table created! Processed {df_gold_country_metrics.count()} geographical markets.")
display(df_gold_country_metrics)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Inspect table details and verify format is DELTA
# MAGIC DESCRIBE DETAIL workspace.retail_lakehouse.gold_country_sales_metrics;
# MAGIC
# MAGIC -- 2. Optimize Delta Lake file compaction for fast query performance
# MAGIC OPTIMIZE workspace.retail_lakehouse.gold_country_sales_metrics;
# MAGIC
# MAGIC -- 3. Query top performing markets
# MAGIC SELECT 
# MAGIC     country,
# MAGIC     total_orders,
# MAGIC     unique_customers,
# MAGIC     total_revenue,
# MAGIC     avg_order_value
# MAGIC FROM workspace.retail_lakehouse.gold_country_sales_metrics
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

