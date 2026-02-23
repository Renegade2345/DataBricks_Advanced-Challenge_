# Databricks notebook source
from pyspark.sql import functions as F

# BRONZE
raw = spark.table("workspace.ecommerce.events_clean_day4")

bronze = raw.withColumn("ingestion_ts", F.current_timestamp())

bronze.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.bronze_events")


# SILVER
bronze_df = spark.table("workspace.ecommerce.bronze_events")

silver = bronze_df.filter(
    (F.col("price") > 0) & (F.col("price") < 10000)
).dropDuplicates(
    ["user_session", "event_time"]
).withColumn(
    "event_date",
    F.to_date("event_time")
).withColumn(
    "price_tier",
    F.when(F.col("price") < 10, "budget")
     .when(F.col("price") < 50, "mid")
     .otherwise("premium")
)

silver.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.silver_events")


# GOLD
silver_df = spark.table("workspace.ecommerce.silver_events")

product_perf = silver_df.groupBy("product_id").agg(
    F.countDistinct(
        F.when(F.col("event_type") == "view", F.col("user_id"))
    ).alias("views"),

    F.countDistinct(
        F.when(F.col("event_type") == "purchase", F.col("user_id"))
    ).alias("purchases"),

    F.sum(
        F.when(F.col("event_type") == "purchase", F.col("price"))
    ).alias("revenue")

).withColumn(
    "conversion_rate",
    (F.col("purchases") / F.col("views")) * 100
)
product_perf = silver_df.groupBy("product_id").agg(

    F.countDistinct(
        F.when(F.col("event_type") == "view", F.col("user_id"))
    ).alias("views"),

    F.countDistinct(
        F.when(F.col("event_type") == "purchase", F.col("user_id"))
    ).alias("purchases"),

    F.sum(
        F.when(F.col("event_type") == "purchase", F.col("price"))
    ).alias("revenue")

).withColumn(
    "conversion_rate",
    F.when(
        F.col("views") > 0,
        (F.col("purchases") / F.col("views")) * 100
    ).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Day 6 Task Summary Paragraph — Medallion Architecture Implementation (Bronze, Silver, Gold)
# MAGIC
# MAGIC On Day 6, I designed and implemented a complete Medallion Architecture pipeline using Databricks and Apache Spark, structuring data into Bronze, Silver, and Gold layers to support scalable analytics and business intelligence. I created the Bronze layer by ingesting raw event data and adding ingestion metadata to preserve source fidelity and enable traceability. In the Silver layer, I applied data cleaning, validation, and transformation techniques, including filtering invalid records, removing duplicates, and deriving new attributes such as event_date and price_tier to improve data quality and usability. Finally, in the Gold layer, I built business-level aggregations such as product performance metrics, including views, purchases, revenue, and conversion rates, enabling meaningful insights for downstream analytics and decision-making. This layered architecture ensures data reliability, scalability, and clear separation of concerns, aligning with industry-standard data engineering practices for building production-grade analytics and AI-ready pipelines.