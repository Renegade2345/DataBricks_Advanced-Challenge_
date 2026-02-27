# Databricks notebook source
# Load existing table
events = spark.table("workspace.ecommerce.events_delta")

# Simulate new incoming records (sample 1000 rows)
new_records = events.limit(1000)

# Append to Delta table
new_records.write.format("delta") \
.mode("append") \
.saveAsTable("workspace.ecommerce.events_delta")

print("New records appended successfully")

# COMMAND ----------

spark.sql("DESCRIBE HISTORY workspace.ecommerce.events_delta").show(truncate=False)

# COMMAND ----------

old_version = spark.read.format("delta") \
.option("versionAsOf", 0) \
.table("workspace.ecommerce.events_delta")

print("Old version count:", old_version.count())

# COMMAND ----------

current = spark.table("workspace.ecommerce.events_delta")

print("Current version count:", current.count())

difference = current.count() - old_version.count()

print("New records added:", difference)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC On Day 11, I implemented Delta Lake versioning and time travel capabilities using Databricks. I simulated incremental data ingestion by appending new records to an existing Delta table and validated transactional integrity. I then leveraged Delta’s built-in versioning to query historical table versions using versionAsOf, enabling point-in-time data retrieval. Finally, I compared row counts between historical and current versions to quantify incremental changes. This exercise demonstrated Delta Lake’s ACID guarantees, data lineage tracking, and time travel capabilities, which are critical for production-grade data governance and auditability.