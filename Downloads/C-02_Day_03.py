# Widget for process date
dbutils.widgets.text("process_date", "2024-01-01")
process_date = dbutils.widgets.get("process_date")

print("Processing for date:", process_date)

from pyspark.sql import functions as F

def load_events():
    return spark.table("workspace.ecommerce.events_delta")

def create_user_features(events_df):
    return (
        events_df.groupBy("user_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type")=="purchase",1).otherwise(0)).alias("purchases"),
            F.sum("price").alias("total_spent"),
            F.avg("price").alias("avg_price")
        )
        .dropDuplicates(["user_id"])
    )

def validate_features(df):
    print("Row count:", df.count())
    df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).show()

def save_silver(df):
    df.write.format("delta") \
      .mode("overwrite") \
      .saveAsTable("workspace.ecommerce.user_features_silver")


    events = load_events()
features_df = create_user_features(events)
validate_features(features_df)
save_silver(features_df)

print("Day 3 pipeline completed successfully")
# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC On Day 3, I productionized the Silver-layer feature engineering pipeline by introducing notebook parameterization using Databricks widgets, modularizing feature creation logic into reusable functions, and configuring a scheduled Databricks Workflow job. This enabled dynamic execution, improved code maintainability, and automated daily feature computation. By transitioning from manual notebook execution to an orchestrated and scheduled pipeline, the system moved closer to production-grade data engineering standards.Medallion architecture and real-world data engineering best practices.
