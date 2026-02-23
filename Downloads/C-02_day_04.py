# Databricks notebook source
from pyspark.sql import functions as F

events = spark.table("workspace.ecommerce.events_delta")

# Convert to Delta managed table
events.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.events_delta_day4")

# SQL Delta table creation
spark.sql("""
CREATE TABLE workspace.ecommerce.events_delta_sql
USING DELTA
AS SELECT * FROM workspace.ecommerce.events_delta_day4
""")

# Schema enforcement test
try:
    wrong_schema = spark.createDataFrame(
        [("a","b","c")],
        ["wrong1","wrong2","wrong3"]
    )

    wrong_schema.write.format("delta") \
    .mode("append") \
    .saveAsTable("workspace.ecommerce.events_delta_day4")

except Exception as e:
    print("Schema enforcement working:", e)

# Remove duplicates
deduplicated_events = events.dropDuplicates(
    ["user_id", "product_id", "event_time"]
)

deduplicated_events.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.events_clean_day4")

# COMMAND ----------

# MAGIC %md
# MAGIC On Day 4, I worked on strengthening data reliability and integrity by implementing Delta Lake schema enforcement and duplicate handling using Databricks and Apache Spark. I converted the dataset into managed Delta tables using both PySpark and SQL approaches within Unity Catalog, ensuring ACID compliance and governed storage. I then tested Delta Lake’s schema enforcement by attempting to insert data with an incompatible schema, which was correctly rejected, demonstrating Delta’s ability to protect data consistency. Additionally, I handled potential duplicate records by applying deduplication logic based on business-critical keys such as user_id, product_id, and event_time, and stored the cleaned dataset as a new managed Delta table. This process ensured that the data pipeline remains reliable, consistent, and production-ready, aligning with industry best practices for building robust and scalable data engineering workflows.