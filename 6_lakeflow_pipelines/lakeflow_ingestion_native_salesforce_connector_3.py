# Databricks notebook source
# MAGIC %md
# MAGIC **A fully managed, connector-based ingestion pipeline that loads SaaS data into the Bronze layer of the Databricks Lakehouse with built-in incremental sync.<br>
# MAGIC
# MAGIC Here we are using Lakeflow Native Salesforce ingestion, which provides managed, incremental CDC ingestion from Salesforce objects into Bronze Delta tables governed by Unity Catalog, without writing custom ingestion code
# MAGIC
# MAGIC - Salesforce account creation
# MAGIC - Creating Salesforce connection in Databricks
# MAGIC - Creating Lakeflow Ingestion Pipeline
# MAGIC - Selecting Unity Catalog target
# MAGIC - Scheduling & notifications
# MAGIC - Incremental ingestion validation
# MAGIC
# MAGIC Highlights:
# MAGIC - Uses Salesforce system fields (LastModifiedDate)
# MAGIC - Maintains ingestion state internally
# MAGIC - Tracks offsets per object
# MAGIC - Writes optimized Delta files
# MAGIC - Guarantees idempotency
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/6_lakeflow_pipelines/snowflakeingestion.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Lakeflow Native Connector**
# MAGIC Lakeflow connectors are Databricks-managed ingestion connectors that directly connect to operational systems and SaaS platforms. They reduce custom ingestion code and are natively governed.
# MAGIC
# MAGIC **Salesforce Ingestion using Lakeflow Native Connector**
# MAGIC - Generate Free Account & security token from Salesforce
# MAGIC - Create Salesforce Connection in Databricks
# MAGIC - Configure Salesforce Objects for Ingestion
# MAGIC - Follow the detailed instruction given below to achieve the ingestion.
# MAGIC
# MAGIC Refer the slide#77 in the <br>
# MAGIC https://docs.google.com/presentation/d/17YwwoAS2CBbUrwmOaH8R3vcKGVTWK-H_/edit?usp=drive_link&ouid=112811125782165229325&rtpof=true&sd=true
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Follow the above document and insert/update/delete data from salesforce and see whether following things are working seamlessly
# MAGIC --1. CDC (Change Data Capture) - Related more towards capturing data from source
# MAGIC --2. SCD1 (Slowly Changing Dimension 1) implemented naturally. - Related more towards how we are writing the changed data from source, either doing just ins/upd/del or only inserting and deactivating flags (scd2)
# MAGIC select * from catalog3_we47.default.account;
