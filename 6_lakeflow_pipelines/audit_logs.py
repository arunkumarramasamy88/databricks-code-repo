# Databricks notebook source
# MAGIC %sql
# MAGIC --drop table if exists catalog1_we47.schema1_we47.audit_log;
# MAGIC create table if not exists catalog1_we47.schema1_we47.audit_log(
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   message string,loadts timestamp)
# MAGIC using delta;

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC insert into catalog1_we47.schema1_we47.audit_log(message,loadts) values('No data in the source DB table',current_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog1_we47.schema1_we47.audit_log
