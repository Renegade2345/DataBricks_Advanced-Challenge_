# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# STEP 1 — Load target Delta table (Day 4 clean table)
target_table_name = "workspace.ecommerce.events_clean_day4"

deltaTable = DeltaTable.forName(
    spark,
    target_table_name
)

print("Loaded target Delta table")


# STEP 2 — Simulate incremental updates (using existing data sample)
updates = spark.table(target_table_name) \
    .orderBy(F.desc("event_time")) \
    .limit(100)

print("Incremental updates dataset created")
updates.show(5)


# STEP 3 — Perform incremental MERGE (UPSERT)
deltaTable.alias("t").merge(
    updates.alias("s"),
    "t.user_session = s.user_session AND t.event_time = s.event_time"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("MERGE operation completed successfully")


# STEP 4 — View Delta table history (Time Travel metadata)
print("Table history:")
spark.sql(f"""
DESCRIBE HISTORY {target_table_name}
""").show(truncate=False)


# STEP 5 — Read historical version using Time Travel
print("Reading Version 0 of table:")
version_0_df = spark.read.format("delta") \
.option("versionAsOf", 0) \
.table(target_table_name)

version_0_df.show(5)


# STEP 6 — Optimize table with ZORDER (performance optimization)
print("Optimizing table...")
spark.sql(f"""
OPTIMIZE {target_table_name}
ZORDER BY (event_type, user_id)
""")

print("Optimization complete")


# STEP 7 — Vacuum old files (cleanup)
print("Running VACUUM cleanup...")
spark.sql(f"""
VACUUM {target_table_name}
RETAIN 168 HOURS
""")

print("VACUUM complete")


# STEP 8 — Verify final table
print("Final table preview:")
spark.table(target_table_name).show(10)

print("Day 5 tasks completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC On Day 5, I implemented incremental data processing and advanced Delta Lake management features using Databricks and Apache Spark. I performed an incremental MERGE (upsert) operation on a managed Delta table to simulate real-world streaming or batch updates, ensuring that existing records were updated and new records were inserted without duplication. This demonstrated how Delta Lake supports efficient incremental pipelines critical for scalable data engineering workflows. I also explored Delta Lake’s Time Travel capability by querying historical versions of the table, enabling data auditing, rollback, and version tracking. To improve performance, I applied OPTIMIZE with ZORDER to reorganize data files for faster query execution, and used VACUUM to safely remove obsolete files and reduce storage overhead. By completing these steps, I strengthened the pipeline’s efficiency, reliability, and maintainability, aligning with production-grade best practices for building scalable and optimized data platforms.