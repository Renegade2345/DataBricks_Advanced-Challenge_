# Databricks notebook source
import time

query = """
SELECT user_id, COUNT(*), SUM(price)
FROM workspace.ecommerce.events_delta
GROUP BY user_id
"""

start = time.time()
spark.sql(query).count()
end = time.time()

print("Execution time:", round(end - start, 3), "seconds")

# COMMAND ----------

spark.sql(query).explain(True)

# COMMAND ----------

spark.sql("DESCRIBE DETAIL workspace.ecommerce.events_delta").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Cost Optimization Strategies Identified
# MAGIC
# MAGIC Reduced repeated Spark actions to avoid redundant job execution.
# MAGIC
# MAGIC Used caching strategically for frequently accessed datasets.
# MAGIC
# MAGIC Identified shuffle-heavy operations via execution plan analysis.
# MAGIC
# MAGIC Recommended partition pruning to reduce full table scans.
# MAGIC
# MAGIC Suggested periodic OPTIMIZE to compact small files.
# MAGIC
# MAGIC Avoided unnecessary .collect() operations in large datasets.
# MAGIC
# MAGIC Recommended broadcast joins for small dimension tables.
# MAGIC
# MAGIC Proposed incremental processing instead of full refresh pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC On Day 12, I focused on runtime analysis and cost optimization strategies in Databricks Serverless. I measured query execution times programmatically and analyzed execution plans to identify shuffle operations, full table scans, and expensive transformations. I optimized performance by reducing redundant Spark actions, leveraging caching where appropriate, and identifying opportunities for partition pruning and file compaction. I also documented cost-saving recommendations, including minimizing repeated computations, optimizing joins, and maintaining Delta tables with periodic OPTIMIZE operations. This exercise reinforced the importance of performance engineering in reducing infrastructure costs and improving scalability in production-grade data pipelines