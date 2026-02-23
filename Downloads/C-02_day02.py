# Databricks notebook source


from pyspark.sql import functions as F

events = spark.table("workspace.ecommerce.events_delta")

features_df = events.groupBy("user_id").agg(
    F.count("*").alias("total_events"),
    
    # count purchases only
    F.count(
        F.when(F.col("event_type") == "purchase", True)
    ).alias("purchases"),
    
    # total money spent
    F.sum("price").alias("total_spent"),
    
    # average price per event
    F.avg("price").alias("avg_price")
)


features_df = features_df.dropDuplicates(["user_id"])


print("Total users:", features_df.count())

print("Null check:")
features_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in features_df.columns
]).show()


features_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.ecommerce.user_features_silver")


print("Sample data from Silver table:")
spark.sql("""
SELECT * 
FROM workspace.ecommerce.user_features_silver
LIMIT 10
""").show()

print("SUCCESS: Silver layer user_features created")

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2 Task Completion Summary — Silver Layer Feature Engineering
# MAGIC
# MAGIC On Day 2, I implemented a user-level feature engineering pipeline using Databricks and Apache Spark, transforming raw event-level data from the Bronze Delta table into structured, analytics-ready features in the Silver layer. I read optimized event data from a managed Delta table in Unity Catalog and performed aggregations to generate meaningful behavioral features per user, including total events, number of purchases, total spend, and average spend. These transformations converted granular event logs into clean, user-centric feature tables suitable for downstream analytics and machine learning workloads.
# MAGIC
# MAGIC To ensure data reliability and quality, I explicitly validated the dataset by checking for duplicate user records and verifying null values across all feature columns. I then stored the resulting feature table as a managed Delta table using Unity Catalog, ensuring ACID compliance, schema enforcement, and scalable storage aligned with the Medallion architecture. By the end of Day 2, I had successfully built a production-grade Silver layer feature table that serves as a reliable foundation for advanced analytics, reporting, and AI-driven applications.