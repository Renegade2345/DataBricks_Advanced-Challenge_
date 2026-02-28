# Databricks notebook source
                ┌────────────────────┐
                │   Raw CSV Data     │
                └─────────┬──────────┘
                          ▼
                ┌────────────────────┐
                │ Bronze Layer       │
                │ events_delta       │
                └─────────┬──────────┘
                          ▼
                ┌────────────────────┐
                │ Silver Layer       │
                │ user_features      │
                └─────────┬──────────┘
                          ▼
         ┌────────────────────────────────┐
         │ ML Training + MLflow Tracking │
         └─────────┬──────────────────────┘
                   ▼
         ┌────────────────────────────────┐
         │ Gold Layer                     │
         │ predictions + recommendations  │
         └─────────┬──────────────────────┘
                   ▼
         ┌────────────────────────────────┐
         │ Business Dashboard / Targeting │
         └────────────────────────────────┘

# COMMAND ----------

# MAGIC %md
# MAGIC The pipeline starts with the Bronze layer where raw data is ingested exactly as it comes, just like unloading fresh vegetables in a mandi. At this stage, schema enforcement ensures everything follows a proper structure, and the data is stored in Delta format to maintain reliability and ACID consistency. Then in the Silver layer, the real cleaning begins, duplicates are removed, data is validated, and meaningful user-level features are created so that the dataset becomes analysis-ready. After that comes the ML training phase, where we define the target label, train models like Logistic Regression and Random Forest, track experiments using MLflow, and evaluate performance using AUC to ensure the model is actually learning something useful. Once the model is validated, we move to the Gold layer, where all users are scored, predictions are saved, and personalized recommendations are generated. Finally, in the optimization phase, we cache heavy queries to speed up performance, apply OPTIMIZE and ZORDER to improve storage efficiency, run VACUUM to clean unused files, and even use time travel when needed to inspect previous data versions — ensuring the entire system is production-grade, efficient, and scalable.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This is where the system moves from being a project to becoming real engineering. The retraining strategy is designed to keep the model adaptive and reliable in production. First, we implement time-based retraining, where the model is automatically retrained every week using the most recent 30 days of data to ensure it captures fresh user behavior patterns. Second, we introduce performance-based retraining by continuously monitoring AUC; if the AUC drops by more than 5%, it signals model degradation and automatically triggers retraining. Third, we monitor data drift — if the distribution of critical features like average price or total events shifts significantly, retraining is initiated to prevent stale predictions. In production, the flow is structured and controlled: new data enters the system, features are updated, the model is retrained, and its AUC is evaluated. If the new model performs better than the existing one, it is promoted in MLflow and replaces the current production model. This ensures the deployed model is always performance-driven, drift-aware, and continuously improving rather than static.