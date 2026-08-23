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
# MAGIC   updated_at
# MAGIC FROM gcp_mysql_fc_we471.logistics.shipments1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO catalog1_we47.schema1_we47.bronze_shipments1 AS target
# MAGIC USING (
# MAGIC   SELECT * FROM gcp_mysql_fc_we471.logistics.shipments1 
# MAGIC   WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM catalog1_we47.schema1_we47.bronze_shipments1)
# MAGIC ) AS source
# MAGIC ON target.shipment_id = source.shipment_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     target.first_name = source.first_name,
# MAGIC     target.last_name = source.last_name,
# MAGIC     target.age = source.age,
# MAGIC     target.role = source.role,
# MAGIC     target.updated_at = source.updated_at
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (shipment_id, first_name, last_name, age, role, updated_at)
# MAGIC   VALUES (source.shipment_id, source.first_name, source.last_name, source.age, source.role, source.updated_at);
