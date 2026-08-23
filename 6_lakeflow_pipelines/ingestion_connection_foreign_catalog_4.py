# Databricks notebook source
# MAGIC %md
# MAGIC ###Foreign Catalog Connection
# MAGIC A Foreign Catalog is a Unity Catalog object that allows Databricks to reference and query metadata that lives outside Databricks, in an external metastore or database, without copying the data.
# MAGIC
# MAGIC Databricks can securely access an external database (Google Cloud SQL) using Unity Catalog–managed connections and foreign catalogs, without ingesting or copying the data.
# MAGIC
# MAGIC **Use Foreign Catalog when:**
# MAGIC - Ad-hoc analysis
# MAGIC - Alternative for Custom JDBC or 3rd party connectors (for data copy)
# MAGIC - Incremental Ingestion (I just need only data from yesterday)
# MAGIC - No need to persist data (If we only want to refer/lookup)
# MAGIC - Real-time lookup (If any changes made in source DB, will be reflected)
# MAGIC - Avoid data duplication (Acts like a shallow copy (without snapshot))

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/6_lakeflow_pipelines/connection_foreign_catalog.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Source DB Side**
# MAGIC CREATE TABLE shipments (
# MAGIC     shipment_id INT PRIMARY KEY,
# MAGIC     first_name  VARCHAR(50),
# MAGIC     last_name   VARCHAR(50),
# MAGIC     age         INT,
# MAGIC     role        VARCHAR(50),
# MAGIC     insert_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC     update_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
# MAGIC
# MAGIC INSERT INTO shipments
# MAGIC (shipment_id, first_name, last_name, age, role)
# MAGIC VALUES
# MAGIC (5000001, 'Rajesh',  'Kumar', 35, 'Driver'),
# MAGIC (5000002, 'Anita',   'Sharma',29, 'Dispatcher'),
# MAGIC (5000003, 'Michael', 'Chen',  41, 'Warehouse Manager'),
# MAGIC (5000004, 'Suresh',  NULL,    52, 'Loader'),
# MAGIC (5000005, 'Priya',   'Iyer',  27, 'Analyst');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC MySQL (shipments table)-  (Foreign Catalog) - Databricks (External Table (foreign catalog)) -> CDC Filter (insert_ts / update_ts) -> Bronze Delta Table 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Source DB Readiness

# COMMAND ----------

# MAGIC %md
# MAGIC **Create the following table in source Database**
# MAGIC create database if not exists logistics;
# MAGIC
# MAGIC CREATE TABLE logistics.shipments (
# MAGIC   shipment_id INT PRIMARY KEY,
# MAGIC   first_name  VARCHAR(50),
# MAGIC   last_name   VARCHAR(50),
# MAGIC   age         INT,
# MAGIC   role        VARCHAR(50),
# MAGIC   updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC
# MAGIC INSERT INTO logistics.shipments VALUES
# MAGIC (5000001,'Rajesh','Kumar',35,'Driver',CURRENT_TIMESTAMP),
# MAGIC (5000002,'Anita','Sharma',29,'Dispatcher',CURRENT_TIMESTAMP),
# MAGIC (5000003,'Michael','Chen',41,'Warehouse Manager',CURRENT_TIMESTAMP),
# MAGIC (5000004,'Suresh',NULL,52,'Loader',CURRENT_TIMESTAMP),
# MAGIC (5000005,'Priya','Iyer',27,'Analyst',CURRENT_TIMESTAMP);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Create Connection & Foreign Catalog
# MAGIC **Foreign Catalog** is a Unity Catalog object that allows Databricks to reference and query metadata that lives outside Databricks, in an external metastore or database, without copying the data.
# MAGIC
# MAGIC **Connection**
# MAGIC - Avoid hard-coding usernames/passwords in notebooks
# MAGIC - Enable centralized governance via Unity Catalog
# MAGIC - Allow multiple users and tables to reuse the same connection
# MAGIC - Support SQL-based external access

# COMMAND ----------

# MAGIC %sql
# MAGIC --Once for all activity
# MAGIC /*CREATE CONNECTION gcp_mysql_conn_wd36
# MAGIC TYPE mysql
# MAGIC OPTIONS (
# MAGIC   host '34.123.166.158',
# MAGIC   port '3306',
# MAGIC   user 'devuser',
# MAGIC   password 'lets hide this password before commit to git'
# MAGIC );
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC --Activity to create a foreign catalog to refresh/refer the metadata
# MAGIC /*CREATE FOREIGN CATALOG gcp_mysql_fc_wd361
# MAGIC USING CONNECTION gcp_mysql_conn_wd36;
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC --Data came from external database using connection+foreign catalog created earlier
# MAGIC --select * from gcp_mysql_fc_wd36.logistics.shipments1;

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Bronze table (Incremental ingestion)
# MAGIC Let me simply use Foreign catalog as a data ingestion mechanism (rather than using traditional JDBC or third party tools to ingest data from Database).

# COMMAND ----------

# MAGIC %sql
# MAGIC --Historical (Onetime) load (Depends on the business requirement)
# MAGIC --If my project requires entire 3 years shipment data from external database  or if we need only one year, apply filters accordingly..
# MAGIC CREATE TABLE IF NOT EXISTS catalog3_we47.schema3_we47.bronze_shipments1
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
# MAGIC --select * from catalog3_we47.schema3_we47.bronze_shipments1

# COMMAND ----------

# MAGIC %md
# MAGIC Try to achieve SCD Type2 (just like that)
# MAGIC ####Insert data into the source Database table and run the incremental load
# MAGIC INSERT INTO logistics.shipments1 VALUES (5000006,'Bala','Chander',35,'DE',CURRENT_TIMESTAMP);
# MAGIC ####Update data into the source Database table and run the incremental load
# MAGIC update logistics.shipments1 set role='Databricks Data Engineer',updated_at=CURRENT_TIMESTAMP where shipment_id=5000006;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COALESCE(MAX(updated_at), '1970-01-01')
# MAGIC   FROM catalog3_we47.schema3_we47.bronze_shipments1

# COMMAND ----------

# MAGIC %sql
# MAGIC --Incremental data ingestion/load of newly added/updated data is insert
# MAGIC --Extraction - We are doing Change Data Capture (CDC) (Inserted/updated)
# MAGIC --Load - We are doing Slowly Changing Dimension Type 2
# MAGIC INSERT INTO catalog3_we47.schema3_we47.bronze_shipments1
# MAGIC SELECT
# MAGIC   shipment_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   age,
# MAGIC   role,
# MAGIC   updated_at
# MAGIC FROM gcp_mysql_fc_we471.logistics.shipments1
# MAGIC WHERE updated_at >
# MAGIC (
# MAGIC   SELECT COALESCE(MAX(updated_at), '1970-01-01')
# MAGIC   FROM catalog3_we47.schema3_we47.bronze_shipments1);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog3_we47.schema3_we47.bronze_shipments1

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,row_number() over(partition by shipment_id order by updated_at desc) rno from catalog3_we47.schema3_we47.bronze_shipments1
# MAGIC qualify rno>1;--latest version or history you can access

# COMMAND ----------

# MAGIC %sql
# MAGIC --Incremental load MERGE for SCD Type 1
# MAGIC MERGE INTO catalog3_we47.schema3_we47.bronze_shipments1 AS target
# MAGIC USING gcp_mysql_fc_we471.logistics.shipments1 AS source
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
# MAGIC   VALUES (source.shipment_id, source.first_name, source.last_name, source.age, source.role, source.updated_at)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Interview Question? How to delete rows from table A which is not present in another table B, using some id?
# MAGIC --I can use left join + subquery to achieve it or I can use merge delete to achieve it
# MAGIC delete from catalog3_we47.schema3_we47.bronze_shipments1 
# MAGIC where shipment_id in (select tgt.shipment_id from catalog3_we47.schema3_we47.bronze_shipments1 tgt left join gcp_mysql_fc_we471.logistics.shipments1 src on tgt.shipment_id=src.shipment_id where src.shipment_id is null);
# MAGIC --Try with anti join

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog3_we47.schema3_we47.bronze_shipments1

# COMMAND ----------

# MAGIC %md
# MAGIC **We will learn a complete VVV Important Cycle of CDC to CDF to SCD1 & SCD2 features when we move to Cloud, without too much of coding offered by Databricks**
