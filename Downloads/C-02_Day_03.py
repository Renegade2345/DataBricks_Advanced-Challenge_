# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

events = spark.table("workspace.ecommerce.events_delta")

# COMMAND ----------

user_features = spark.table("workspace.ecommerce.user_features_silver")

events_enriched = events.join(
    user_features,
    on="user_id",
    how="left"
)

# COMMAND ----------

window_spec = Window.partitionBy("user_id").orderBy("event_time")

events_with_running = events_enriched.withColumn(
    "cumulative_events",
    F.count("*").over(window_spec)
).withColumn(
    "cumulative_spent",
    F.sum("price").over(window_spec)
)

# COMMAND ----------

top_products = events.filter(F.col("event_type") == "purchase") \
    .groupBy("product_id") \
    .agg(F.sum("price").alias("revenue")) \
    .orderBy(F.desc("revenue")) \
    .limit(5)

# COMMAND ----------

conversion = events.groupBy("category_code") \
    .pivot("event_type") \
    .count() \
    .withColumn(
        "conversion_rate",
        (F.col("purchase") / F.col("view")) * 100
    )

# COMMAND ----------

derived_features = events_with_running.withColumn(
    "is_high_value_user",
    F.when(F.col("cumulative_spent") > 10000, 1).otherwise(0)
).withColumn(
    "purchase_frequency",
    F.col("purchases") / F.col("total_events")
)

# COMMAND ----------

derived_features.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.events_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Day 3 Task Completion Summary — Gold Layer Transformations & Advanced Analytics
# MAGIC
# MAGIC On Day 3, I built an advanced transformation pipeline using Databricks and Apache Spark to enrich event-level data and create analytics-ready datasets in the Gold layer. I loaded the full ecommerce dataset from managed Delta tables in Unity Catalog and performed complex joins between event-level data and user-level Silver layer feature tables to create a unified and enriched dataset. This integration enabled deeper behavioral analysis by combining granular events with aggregated user features.
# MAGIC
# MAGIC I then applied window functions to calculate running metrics such as cumulative events and cumulative spend per user, allowing tracking of user activity over time and enabling advanced analytical use cases like lifetime value analysis and behavioral segmentation. Additionally, I generated business-critical aggregations such as top products by revenue and category-level conversion rates, and created derived features like high-value user indicators to enhance downstream analytics and AI readiness.
# MAGIC
# MAGIC Finally, I stored the enriched dataset as a managed Delta table in the Gold layer using Unity Catalog, ensuring ACID compliance, optimized storage, and scalable access. By the end of Day 3, I had successfully implemented a production-grade Gold layer pipeline that transforms structured Silver layer data into enriched, analytics-ready datasets aligned with Medallion architecture and real-world data engineering best practices.