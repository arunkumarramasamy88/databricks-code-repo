# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will help us check the count of the incremental data present in some foreign table, based on the count set in the paramter row_count using taskValues, I am going to decide whether to run the subsequent ETL tasks/Pipline or not..

# COMMAND ----------

row_count=spark.sql("""select * from gcp_mysql_fc_we471.logistics.shipments1 where updated_at>(SELECT COALESCE(MAX(updated_at), '1970-01-01')  FROM catalog1_we47.schema1_we47.bronze_shipments1)""").count()
print(row_count)

# COMMAND ----------

#This is a job.task level property to set a parameter/variable inside a task, which can be propogated/read by other tasks on the same lakeflow job
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
