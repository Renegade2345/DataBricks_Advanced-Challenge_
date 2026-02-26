# Databricks notebook source

# DAY 10 — Serverless-Safe Query Optimization and Caching


import time


# STEP 1 — Define heavy aggregation query
query = """
SELECT 
    user_id,
    COUNT(*) as total_events,
    SUM(price) as total_spent,
    AVG(price) as avg_price
FROM workspace.ecommerce.events_delta
WHERE event_type = 'purchase'
GROUP BY user_id
"""


# STEP 2 — Explain Plan Analysis
print("\n===== EXPLAIN PLAN =====")
spark.sql(query).explain(True)


# STEP 3 — Run WITHOUT cache
print("\n===== RUNNING WITHOUT CACHE =====")

no_cache_times = []

for i in range(3):
    start = time.time()
    
    spark.sql(query).count()
    
    execution_time = time.time() - start
    no_cache_times.append(execution_time)
    
    print(f"Run {i+1} WITHOUT cache: {execution_time:.3f} seconds")


avg_no_cache = sum(no_cache_times) / len(no_cache_times)


# STEP 4 — Enable Serverless-compatible cache
print("\n===== ENABLING CACHE =====")

spark.sql("CACHE SELECT * FROM workspace.ecommerce.events_delta")

print("Cache enabled successfully")


# STEP 5 — Run WITH cache
print("\n===== RUNNING WITH CACHE =====")

cache_times = []

for i in range(3):
    start = time.time()
    
    spark.sql(query).count()
    
    execution_time = time.time() - start
    cache_times.append(execution_time)
    
    print(f"Run {i+1} WITH cache: {execution_time:.3f} seconds")


avg_cache = sum(cache_times) / len(cache_times)


# STEP 6 — Compare performance
print("\n===== PERFORMANCE COMPARISON =====")

print(f"Average WITHOUT cache: {avg_no_cache:.3f} seconds")
print(f"Average WITH cache: {avg_cache:.3f} seconds")

improvement = ((avg_no_cache - avg_cache) / avg_no_cache) * 100

print(f"Performance improvement: {improvement:.2f}%")


# STEP 7 — Display sample output
print("\n===== SAMPLE RESULT =====")

display(spark.sql(query).limit(10))



# COMMAND ----------

# MAGIC %md
# MAGIC On Day 10, I focused on Spark query performance optimization by analyzing execution plans and implementing caching strategies in a Unity Catalog Serverless environment. I executed aggregation-heavy queries on the Delta table, examined the logical and physical execution plans using the explain function, and identified Photon engine optimizations such as predicate pushdown and efficient columnar scanning. I then enabled Serverless-compatible SQL caching to store frequently accessed data in memory and compared execution times before and after caching. This demonstrated how Spark caching and execution plan analysis improve query performance, reduce disk I/O, and optimize distributed data processing for scalable, production-grade analytics pipelines.