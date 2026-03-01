# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import mlflow
import mlflow.spark

# ---------- DATA PIPELINE ----------

# Load events
events = spark.table("workspace.ecommerce.events_delta")

# Feature Engineering (Silver logic)
features_df = (
    events.groupBy("user_id")
    .agg(
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type")=="purchase",1).otherwise(0)).alias("purchases"),
        F.sum("price").alias("total_spent"),
        F.avg("price").alias("avg_price")
    )
)

# Create label
label_df = (
    events.groupBy("user_id")
    .agg(F.max(
        F.when(F.col("event_type")=="purchase",1).otherwise(0)
    ).alias("purchased"))
)

training_data = features_df.join(label_df,"user_id")

# COMMAND ----------

# Assemble features
assembler = VectorAssembler(
    inputCols=["total_events","purchases","total_spent","avg_price"],
    outputCol="features"
)

data = assembler.transform(training_data).select("features","purchased")

# Train/Test split
train, test = data.randomSplit([0.8,0.2], seed=42)

evaluator = BinaryClassificationEvaluator(
    labelCol="purchased",
    metricName="areaUnderROC"
)

with mlflow.start_run(run_name="Final_Production_Model"):

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="purchased",
        numTrees=100,
        maxDepth=12
    )

    model = rf.fit(train)
    predictions = model.transform(test)

    auc = evaluator.evaluate(predictions)

    mlflow.log_param("numTrees",100)
    mlflow.log_param("maxDepth",12)
    mlflow.log_metric("AUC",auc)

    mlflow.spark.log_model(
        model,
        artifact_path="final_model",
        dfs_tmpdir="/Volumes/workspace/ecommerce/ecommerce_data/mlflow_tmp"
    )

    print("Final model saved")
    print("Final AUC:", auc)

# COMMAND ----------

from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

# Convert probability vector to array
final_predictions = final_predictions.withColumn(
    "probability_array",
    vector_to_array("probability")
)

# Extract probability of class 1
gold_predictions = final_predictions.select(
    "user_id",
    F.col("probability_array")[1].alias("purchase_probability"),
    "prediction"
)

gold_predictions.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.gold_user_predictions_final")

print("Gold predictions saved successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC On Day 14, I integrated the complete data and machine learning workflows into a unified end-to-end pipeline. The system loads raw event data, performs feature engineering, generates user-level labels, and trains a production-grade Random Forest classification model. The final model was logged and saved using MLflow for reproducibility and lifecycle management. I then executed full-batch inference to score the entire user base and persisted predictions to a Gold-layer Delta table for business consumption. This finalized the complete Medallion-based data architecture combined with an automated ML training and inference pipeline, resulting in a production-ready machine learning system.