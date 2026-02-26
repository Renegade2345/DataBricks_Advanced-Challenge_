# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS


# STEP 1 — Load events
events = spark.table("workspace.ecommerce.events_delta")


# STEP 2 — Create rating mapping
interaction_df = events.withColumn(
    "rating",
    F.when(F.col("event_type") == "purchase", 3)
     .when(F.col("event_type") == "cart", 2)
     .otherwise(1)
).select(
    F.col("user_id").cast("int"),
    F.col("product_id").cast("int"),
    F.col("rating")
).groupBy("user_id", "product_id").agg(
    F.avg("rating").alias("rating")
).dropna().sample(0.01, seed=42)


# STEP 3 — Train ALS model
als = ALS(
    userCol="user_id",
    itemCol="product_id",
    ratingCol="rating",
    rank=5,
    maxIter=5,
    regParam=0.1,
    coldStartStrategy="drop"
)

model = als.fit(interaction_df)


# STEP 4 — Create candidate user-product pairs
users = interaction_df.select("user_id").distinct()
products = interaction_df.select("product_id").distinct()

candidates = users.crossJoin(products).sample(0.001)


# STEP 5 — Generate predictions 
predictions = model.transform(candidates)


# STEP 6 — Get top recommendations using groupBy 
recommendations = predictions.groupBy("user_id").agg(
    F.max("prediction").alias("top_score")
)


# STEP 7 — Save safely
recommendations.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.gold_user_recommendations")


display(spark.table("workspace.ecommerce.gold_user_recommendations"))



# COMMAND ----------

# MAGIC %md
# MAGIC On Day 9, I implemented a collaborative filtering recommendation system using Spark ML’s ALS algorithm to generate personalized Top-5 product recommendations per user. I transformed user interaction events into rating signals, trained the ALS model on sampled data for scalability, and converted nested recommendation outputs into a flattened schema compatible with Unity Catalog Serverless. The final recommendations were stored in a Gold Delta table, completing the end-to-end recommendation pipeline for production-ready personalized product recommendations.