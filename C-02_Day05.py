# DAY 5 — Machine Learning Dataset Preparation

from pyspark.sql import functions as F

# STEP 1 — Load events and features tables
events = spark.table("workspace.ecommerce.events_clean_day4")
features_df = spark.table("workspace.ecommerce.user_features_silver")

print("Loaded events and features tables")


# STEP 2 — Create binary purchase label per user
label_df = events.groupBy("user_id").agg(
    F.max(
        F.when(F.col("event_type") == "purchase", 1).otherwise(0)
    ).alias("purchased")
)

print("Label table created")
display(label_df)


# STEP 3 — Join features with labels
training_data = features_df.join(
    label_df,
    on="user_id",
    how="inner"
)

print("Training dataset created")
display(training_data)


# STEP 4 — Split into train and test datasets
train_df, test_df = training_data.randomSplit(
    [0.8, 0.2],
    seed=42
)

print("Train/Test split completed")

print("Train count:", train_df.count())
print("Test count:", test_df.count())


# STEP 5 — Validate label distribution
print("Label distribution in full dataset:")
training_data.groupBy("purchased").count().show()

print("Label distribution in train dataset:")
train_df.groupBy("purchased").count().show()

print("Label distribution in test dataset:")
test_df.groupBy("purchased").count().show()


# STEP 6 — Save ML datasets as Delta tables
train_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.ml_train_data")

test_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.ecommerce.ml_test_data")

print("ML datasets saved successfully")


# STEP 7 — Verify saved tables
display(spark.table("workspace.ecommerce.ml_train_data"))
display(spark.table("workspace.ecommerce.ml_test_data"))

print("Day 5 ML preparation completed successfully")


# Read stream
stream_df = spark.readStream \
    .schema(events.schema) \
    .csv(input_path)

print("Streaming DataFrame created")


# Write stream using AvailableNow trigger (Serverless fix)
query = stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start(output_path)

print("Streaming started")


# Wait for completion
query.awaitTermination()

print("Streaming completed")


# Query results
result = spark.read.format("delta").load(output_path)

print("Streaming output preview:")
display(result)










On Day 5, I prepared a machine learning–ready dataset by creating a binary purchase label to indicate whether a user had completed a purchase. I joined this label with the Silver layer user feature table to construct a complete feature-label dataset suitable for supervised learning. I then split the dataset into training and testing sets using a reproducible random split to ensure proper model evaluation. To validate data quality, I analyzed the label distribution across the full, training, and testing datasets to confirm balanced and consistent representation. Finally, I stored both training and testing datasets as managed Delta tables in Unity Catalog, ensuring scalable, reliable, and production-ready storage for downstream machine learning workflows
