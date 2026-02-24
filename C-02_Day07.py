# Databricks notebook source
# DBTITLE 1,Cell 1
with mlflow.start_run(run_name="RandomForest_Model_Run"):

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="purchased",
        numTrees=50,
        maxDepth=10
    )

    model = rf.fit(train_data)

    predictions = model.transform(test_data)

    auc = evaluator.evaluate(predictions)

    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("numTrees", 50)
    mlflow.log_param("maxDepth", 10)

    mlflow.log_metric("AUC", auc)

    mlflow.spark.log_model(
        model,
        artifact_path="random_forest_model",
        dfs_tmpdir="/Volumes/workspace/ecommerce/ecommerce_data/mlflow_tmp"
    )

    print("MLflow run completed")
    print("AUC:", auc)

# COMMAND ----------

# MAGIC %md
# MAGIC On Day 7, I implemented experiment tracking using MLflow to manage and monitor machine learning model training. I trained a Random Forest model and logged key parameters such as model type, number of trees, and depth, along with performance metrics including AUC. I also logged the trained model artifact into MLflow, enabling versioned model storage and reproducibility. Using MLflow’s experiment tracking capabilities, I was able to compare multiple runs, analyze performance metrics, and maintain a structured history of model development. This workflow ensures transparency, reproducibility, and efficient model lifecycle management, aligning with industry best practices for production-grade machine learning systems.