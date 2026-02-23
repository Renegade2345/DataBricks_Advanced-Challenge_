# DAY 6 — ML Training, Manual Tuning, and AUC Comparison (Serverless Compatible)

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# STEP 1 — Load datasets
train_df = spark.table("workspace.ecommerce.ml_train_data")
test_df = spark.table("workspace.ecommerce.ml_test_data")

print("Train count:", train_df.count())
print("Test count:", test_df.count())


# STEP 2 — Assemble features
assembler = VectorAssembler(
    inputCols=[
        "total_events",
        "purchases",
        "total_spent",
        "avg_price"
    ],
    outputCol="features"
)

train_data = assembler.transform(train_df).select("features", "purchased")
test_data = assembler.transform(test_df).select("features", "purchased")

print("Features assembled")


# STEP 3 — Train Logistic Regression
lr = LogisticRegression(
    featuresCol="features",
    labelCol="purchased",
    maxIter=20
)

lr_model = lr.fit(train_data)

lr_predictions = lr_model.transform(test_data)

print("Logistic Regression trained")


# STEP 4 — Train multiple Random Forest models (Manual tuning)

rf_configs = [
    {"numTrees": 20, "maxDepth": 5},
    {"numTrees": 50, "maxDepth": 5},
    {"numTrees": 20, "maxDepth": 10},
    {"numTrees": 50, "maxDepth": 10}
]

evaluator = BinaryClassificationEvaluator(
    labelCol="purchased",
    metricName="areaUnderROC"
)

rf_results = []

for config in rf_configs:

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="purchased",
        numTrees=config["numTrees"],
        maxDepth=config["maxDepth"]
    )

    model = rf.fit(train_data)

    predictions = model.transform(test_data)

    auc = evaluator.evaluate(predictions)

    rf_results.append({
        "model": model,
        "numTrees": config["numTrees"],
        "maxDepth": config["maxDepth"],
        "auc": auc,
        "predictions": predictions
    })

    print(f"RF numTrees={config['numTrees']}, maxDepth={config['maxDepth']}, AUC={auc}")


# STEP 5 — Evaluate Logistic Regression
lr_auc = evaluator.evaluate(lr_predictions)

print("\nLogistic Regression AUC:", lr_auc)


# STEP 6 — Find best Random Forest model
best_rf = max(rf_results, key=lambda x: x["auc"])

print("\nBest Random Forest:")
print("numTrees:", best_rf["numTrees"])
print("maxDepth:", best_rf["maxDepth"])
print("AUC:", best_rf["auc"])


# STEP 7 — Compare models
best_auc = max(lr_auc, best_rf["auc"])

if best_auc == lr_auc:
    best_model_name = "Logistic Regression"
    best_predictions = lr_predictions
else:
    best_model_name = "Random Forest"
    best_predictions = best_rf["predictions"]

print("\nBest overall model:", best_model_name)
print("Best overall AUC:", best_auc)


# STEP 8 — Save predictions
best_predictions.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.ml_predictions")

print("\nPredictions saved successfully")


# STEP 9 — Preview predictions
display(spark.table("workspace.ecommerce.ml_predictions"))

print("\nDay 6 ML pipeline completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC On Day 6, I implemented a complete machine learning training and evaluation pipeline using Databricks and Apache Spark ML to predict user purchase behavior. I first transformed the prepared feature dataset into a machine learning–compatible format using VectorAssembler to create feature vectors. I trained a Logistic Regression model as a baseline and then trained multiple Random Forest models with different hyperparameter configurations to improve predictive performance. Since serverless compute has limitations on automated cross-validation, I performed manual hyperparameter tuning by training multiple models and evaluating each using the Area Under the ROC Curve (AUC) metric. I compared the performance of all models to identify the best-performing model and saved the resulting predictions as a managed Delta table for downstream analytics and model deployment. This process demonstrates a complete scalable machine learning workflow integrated with Delta Lake, enabling reliable model training, evaluation, and production-ready data storage.
