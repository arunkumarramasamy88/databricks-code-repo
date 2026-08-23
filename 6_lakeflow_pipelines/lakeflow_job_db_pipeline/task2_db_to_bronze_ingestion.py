# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog1_we47.schema1_we47.bronze_shipments1
# MAGIC USING DELTA
# MAGIC AS SELECT
# MAGIC   shipment_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   age,
# MAGIC   role,
# MAGIC   updated_at,
# MAGIC   city
# MAGIC FROM gcp_mysql_fc_we471.logistics.shipments1;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite table catalog1_we47.schema1_we47.bronze_shipments1
# MAGIC SELECT * FROM gcp_mysql_fc_we471.logistics.shipments1 
# MAGIC   WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM catalog1_we47.schema1_we47.bronze_shipments1);
