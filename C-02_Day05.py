# DAY 4 — Structured Streaming (Serverless-compatible)

from pyspark.sql import functions as F

# Paths
input_path = "/Volumes/workspace/ecommerce/ecommerce_data/stream_input"
output_path = "/Volumes/workspace/ecommerce/ecommerce_data/stream_output"
checkpoint_path = "/Volumes/workspace/ecommerce/ecommerce_data/checkpoints/stream_checkpoint"

# Load source table
events = spark.table("workspace.ecommerce.events_clean_day4")

# Simulate streaming input
events.limit(500).write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", True) \
    .save(input_path)

print("Streaming input folder ready")


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










On Day 4, I implemented a Structured Streaming pipeline using Databricks and Apache Spark to simulate real-time data ingestion and processing. I configured Spark to read streaming data from a folder source, enabling micro-batch processing to simulate continuous event ingestion. I integrated checkpointing to ensure fault tolerance and exactly-once processing semantics, allowing the system to recover reliably from failures without data loss or duplication. The streaming data was written directly into Delta Lake, creating a continuously updated Delta table optimized for scalable analytics. Finally, I queried the streaming output to validate that new records were successfully processed and stored. This implementation demonstrates how Delta Lake and Structured Streaming work together to support reliable, real-time data pipelines aligned with modern data engineering and production-grade streaming architectures.
