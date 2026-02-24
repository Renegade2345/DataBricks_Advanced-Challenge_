# Databricks notebook source
import mlflow

experiments = mlflow.search_experiments()

for exp in experiments:
    print("Name:", exp.name)
    print("ID:", exp.experiment_id)
    print("-----")

# COMMAND ----------

experiment_id = "7379721*****"

runs = mlflow.search_runs(
    experiment_ids=[experiment_id],
    order_by=["start_time DESC"]
)

display(runs)

# COMMAND ----------

# DAY 8 — Score users and save Gold predictions 

import mlflow
import mlflow.spark

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F


# STEP 1 — Load model (use correct run_id)
run_id = "7ac1fc00d7724e36bb5e0faa5*****"

model_uri = f"runs:/{run_id}/random_forest_model"

model = mlflow.spark.load_model(
    model_uri,
    dfs_tmpdir="/Volumes/workspace/ecommerce/ecommerce_data/mlflow_tmp"
)

print("Model loaded successfully")


# STEP 2 — Load features
features_df = spark.table("workspace.ecommerce.user_features_silver")


# STEP 3 — Assemble features
assembler = VectorAssembler(
    inputCols=[
        "total_events",
        "purchases",
        "total_spent",
        "avg_price"
    ],
    outputCol="features"
)

scoring_data = assembler.transform(features_df)


# STEP 4 — Score users
predictions = model.transform(scoring_data)


# STEP 5 — Convert probability vector → array (FIX)
predictions_clean = predictions \
    .withColumn("prob_array", vector_to_array("probability")) \
    .select(
        "user_id",
        F.col("prob_array")[1].alias("purchase_probability"),
        "prediction"
    )


# STEP 6 — Save Gold table
predictions_clean.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.gold_user_predictions")


# STEP 7 — Show results
display(
    spark.table("workspace.ecommerce.gold_user_predictions")
)



# COMMAND ----------

# MAGIC %md
# MAGIC On Day 8, I implemented the machine learning inference pipeline to score all users based on their likelihood of making a purchase. I loaded the trained Random Forest model from MLflow and applied it to the Silver layer user feature table to generate prediction probabilities for each user. I converted the model output into structured prediction scores and stored the results as a managed Delta table in the Gold layer using Unity Catalog. This Gold table contains user-level purchase probabilities and predicted labels, enabling downstream business applications such as targeted marketing and customer prioritization. This step completed the end-to-end ML pipeline by transforming engineered features into production-ready predictions stored in a scalable, governed Delta Lake architecture.