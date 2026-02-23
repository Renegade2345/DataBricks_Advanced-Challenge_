# Databricks notebook source
events = spark.read.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv")

# COMMAND ----------

events = spark.read.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/Volumes/workspace/ecommerce/ecommerce_data/2019-*.csv")

# COMMAND ----------

# DBTITLE 1,Cell 4
# Create managed Delta table
events.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.events_delta")

# Simulate small files
for i in range(3):
    events.limit(500).write.format("delta") \
    .mode("append") \
    .saveAsTable("workspace.ecommerce.events_delta")

# Optimize
spark.sql("OPTIMIZE workspace.ecommerce.events_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC **Day 1 Task Completion Summary — Delta Lake Conversion and Optimization**
# MAGIC
# MAGIC On Day 1, I successfully built a Delta Lake pipeline using Databricks and Apache Spark to transform raw ecommerce event data into a production-grade storage format. First, I ingested large CSV datasets (~13M+ records) from a Unity Catalog Volume into a Spark DataFrame, ensuring proper schema inference and structured processing. Next, I converted this raw data into Delta Lake format using Spark’s Delta writer and created a managed Delta table in Unity Catalog, enabling ACID transactions, schema enforcement, and efficient columnar storage via Parquet. To simulate a real-world ingestion pattern, I appended small batches of data multiple times, intentionally creating the small file problem, which negatively impacts query performance due to file fragmentation. I then executed the OPTIMIZE command to compact small files into larger optimized files, improving query efficiency and storage performance.
# MAGIC
# MAGIC During this process, I encountered and resolved infrastructure challenges related to restricted DBFS root access and Unity Catalog Volume path permissions on serverless compute. I fixed these issues by using Unity Catalog managed tables (saveAsTable) instead of direct filesystem paths, ensuring proper credential-scoped access and compatibility with Databricks' serverless architecture. By the end of Day 1, I had successfully implemented a fully functional Delta Lake table optimized for scalable analytics and production workloads.