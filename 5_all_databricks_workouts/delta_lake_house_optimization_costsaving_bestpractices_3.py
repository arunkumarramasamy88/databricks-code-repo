# Databricks notebook source
# MAGIC %md
# MAGIC ###Deltalake & Lakehouse Optimization Usecases

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/5_all_databricks_workouts/DELTA OPTIMIZATIONS.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Handling Data Skew & Query Performance (Optimize & Z-Order)
# MAGIC Scenario: The analytics team reports that queries filtering silver_shipments by source_city and shipment_date are becoming slow as data volume grows.
# MAGIC
# MAGIC Task: Run the OPTIMIZE command with ZORDER on the silver_shipments table to co-locate related data in the same files.
# MAGIC
# MAGIC Outcome:
# MAGIC Why did we choose source_city and shipment_date for Z-Ordering instead of shipment_id? Think about high cardinality vs. query filtering

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Speeding up Regional Queries (Partition Pruning)
# MAGIC Scenario: The dashboard team reports that queries filtering for orgin_hub_city with "New York" shipments from the gold_core_curated_tbl table are scanning the entire dataset (Terabytes of data), even though New York is only 5% of the data. This is racking up compute costs.
# MAGIC
# MAGIC Task: Re-create the gold_core_curated_tbl table partitioned by orgin_hub_city. Run a query filtering for one city to demonstrate "Partition Pruning" (where Spark skips files that don't match the filter).
# MAGIC
# MAGIC Outcome: Verify the partition filtering is applied or not, by performing explain plan, check for the PartitionFilters in the output.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Storage Cost Savings (Vacuum)
# MAGIC Scenario: Your Project pipeline runs every hour, creating many small files and obsolete versions of data. Your storage costs are rising. You need to clean up files that are no longer needed for time travel.
# MAGIC
# MAGIC Task: Execute a Vacuum command to remove data files older than the retention threshold.
# MAGIC
# MAGIC Outcome: Performance improvement, cost saving, best practices.
# MAGIC
# MAGIC Observation: Perform the describe history and find whether vacuum is completed.

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Modern Data Layout (Liquid Clustering)
# MAGIC Scenario: You are redesigning the silver_shipments table. You want to avoid the "small files" problem and need a flexible layout that adapts to changing query patterns automatically without rewriting the table.
# MAGIC
# MAGIC Task: Re-create the silver_shipments table using Liquid Clustering on the shipment_id column.
# MAGIC
# MAGIC Outcome: Liquid Clustering over traditional partitioning when the cardinality of shipment_id is very high.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Cost Efficient Environment Cloning (Shallow Clone)
# MAGIC Scenario: The QA team needs to test an update on the gold_core_curated_tbl table. The table is 5TB in size. You cannot afford to duplicate the storage cost just for a test and the update should not affect the original table.
# MAGIC
# MAGIC Task: Create a Shallow Clone of the gold table for the QA team.
# MAGIC
# MAGIC Outcome: If we delete records from the source table (gold_core_curated_tbl), will the QA table (gold_core_curated_tbl_qa) be affected & vice versa? Why or why not?

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Disaster Recovery (Time Travel & Restore)
# MAGIC Scenario: A junior data engineer accidentally ran a logic error that corrupted the gold_core_curated_tbl table 15 minutes ago. You need to revert the table to its previous state immediately.
# MAGIC
# MAGIC Task: Use Delta Lake's Restore feature to roll back the table.
# MAGIC
# MAGIC Outcome:What is the difference between querying with VERSION AS OF (Time Travel) and running RESTORE?
