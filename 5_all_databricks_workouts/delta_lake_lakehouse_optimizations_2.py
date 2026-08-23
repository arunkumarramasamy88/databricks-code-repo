# Databricks notebook source
# MAGIC %md
# MAGIC ###Delta Lake (Lakehouse Performance Optimization, Cost Saving & Best Practices)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/5_all_databricks_workouts/DELTA OPTIMIZATIONS.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Performance Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. OPTIMIZE COMMAND
# MAGIC - The OPTIMIZE command in Databricks compacts small files (due to frequent updates, merges, and streaming writes) into larger ones (~1GB) within a Delta table.
# MAGIC - This improves query performance by reducing the number of files that Spark needs to read and reduces metadata overhead.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat1.deltadb

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tblsales
# MAGIC (
# MAGIC   sales_id INT,
# MAGIC   product_id INT,
# MAGIC   region STRING,
# MAGIC   sales_amount DOUBLE,
# MAGIC   sales_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblsales

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tblsales VALUES
# MAGIC   (1, 101, 'North', 1000.50, '2025-10-16'),
# MAGIC   (2, 102, 'South', 500.75, '2025-10-16'),
# MAGIC   (3, 103, 'East', 700.20, '2025-10-16'),
# MAGIC   (4, 104, 'West', 1200.00, '2025-10-16');
# MAGIC
# MAGIC INSERT INTO tblsales VALUES
# MAGIC   (5, 101, 'North', 800.00, '2025-10-17'),
# MAGIC   (6, 102, 'South', 450.00, '2025-10-17'),
# MAGIC   (7, 103, 'East', 600.00, '2025-10-17'),
# MAGIC   (8, 104, 'West', 1100.00, '2025-10-17');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check fragmentation (numFiles & sizeInBytes)
# MAGIC DESCRIBE DETAIL tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Optimize the table
# MAGIC --This performs file compaction:
# MAGIC --Combines many small Parquet files into fewer large files (around 1 GB default).
# MAGIC --Improves read performance and reduces metadata overhead.
# MAGIC OPTIMIZE tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify compaction
# MAGIC -- After optimization, run:
# MAGIC DESCRIBE DETAIL tblsales;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. ZORDER
# MAGIC - ZORDER is an optional feature used with OPTIMIZE to colocate related data physically in the same set of files by sorting and add internal indexing for faster retrival of data.
# MAGIC - Reduces file scan for queries filtering on ZORDER columns using index.
# MAGIC - Works best for columns (low or high cardinal) used frequently in WHERE clauses.
# MAGIC
# MAGIC #### EXAMPLE USE CASE:
# MAGIC - Periodically optimize large Delta tables with frequent writes/updates (optimize will compact)
# MAGIC - Use ZORDER on high-selectivity/filtering columns to improve read performance (ordering and indexing happens behind the scene).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Step 1 – Create the Delta table
# MAGIC use lakehousecat1.deltadb;
# MAGIC CREATE OR REPLACE TABLE customer_txn (
# MAGIC     txn_id INT,
# MAGIC     customer_id INT,
# MAGIC     region STRING,
# MAGIC     txn_amount DOUBLE,
# MAGIC     txn_type STRING,
# MAGIC     transaction_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history customer_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC --Step 2 – Insert multiple small batches
# MAGIC --Each insert writes a few small Parquet files.
# MAGIC -- Batch 1
# MAGIC INSERT INTO customer_txn VALUES
# MAGIC  (1, 1001, 'North', 250.00, 'Online', '2025-10-01'),
# MAGIC  (2, 1002, 'South', 400.00, 'Offline', '2025-10-02'),
# MAGIC  (3, 1003, 'West', 600.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC -- Batch 2
# MAGIC INSERT INTO customer_txn VALUES
# MAGIC  (4, 1001, 'North', 300.00, 'Offline', '2025-10-01'),
# MAGIC  (5, 1004, 'East', 750.00, 'Online', '2025-10-02'),
# MAGIC  (6, 1005, 'South', 180.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC -- Batch 3
# MAGIC INSERT INTO customer_txn VALUES
# MAGIC  (7, 1001, 'North', 270.00, 'Online', '2025-10-01'),
# MAGIC  (8, 1003, 'West', 500.00, 'Offline', '2025-10-02'),
# MAGIC  (9, 1002, 'South', 900.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC /*
# MAGIC --Before optimize or zordering
# MAGIC tablefolder
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3
# MAGIC     - part-4
# MAGIC     - part-5
# MAGIC
# MAGIC select * from customer_txn where region='North; --This query will scan all files and give the result of matching data
# MAGIC
# MAGIC optimize customer_txn;
# MAGIC After optimize
# MAGIC tablefolder
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3
# MAGIC     - part-4
# MAGIC     - part-5 -- All part-0 to part-4 will be kept together in part-5
# MAGIC
# MAGIC select * from customer_txn where region='North; --This query will scan part-5 file alone and give the result of matching data (reduced files operation & metadata management)
# MAGIC
# MAGIC
# MAGIC optimize customer_txn zorder by transaction_date;
# MAGIC
# MAGIC optimize customer_txn;
# MAGIC After optimize
# MAGIC tablefolder
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3
# MAGIC     - part-4
# MAGIC     - part-5 -- All part-0 to part-4 will be kept together in part-5 + sort + colocate + indexing the data rows on transaction_date column
# MAGIC
# MAGIC select * from customer_txn where transaction_date='2025-12-10'; --This query will scan part-5 file alone using the index and scan only the data wrt 2025-12-10 alone without querying the entire data and give the result (reduced files operation + metadata management + faster retrival of required data alone without making full table scan)
# MAGIC */
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history customer_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3 – Inspect fragmentation (numFiles & sizeInBytes)
# MAGIC DESCRIBE DETAIL customer_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4 – Run OPTIMIZE ZORDER - watch out the metrics - zOrderStats
# MAGIC -- Now compact and physically order data with index added.
# MAGIC OPTIMIZE customer_txn ZORDER BY (transaction_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC --look at the operationParameters (zOrderBy)
# MAGIC DESCRIBE HISTORY customer_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3 – Inspect fragmentation
# MAGIC DESCRIBE DETAIL customer_txn;

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Partitioning
# MAGIC Partitioning is the practice of physically splitting a table's data into separate **folders** based on a column.<br>
# MAGIC Good partition columns:<br>
# MAGIC - Low cardinality (low difference columns such as date, age, city, region, gender)
# MAGIC - Columns used Frequently used in filters
# MAGIC - Hard to manage (we can't change the partition columns very frequently)

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat1.deltadb;
# MAGIC CREATE OR REPLACE TABLE customer_txn_part1 (
# MAGIC     txn_id INT,
# MAGIC     customer_id INT,
# MAGIC     region STRING,
# MAGIC     txn_amount DOUBLE,
# MAGIC     txn_type STRING,
# MAGIC     transaction_date DATE
# MAGIC ) 
# MAGIC using delta
# MAGIC partitioned by (transaction_date);
# MAGIC insert into customer_txn_part1 select * from customer_txn;
# MAGIC --or
# MAGIC --create or replace table customer_txn_part partitioned by (transaction_date) as select * from customer_txn;
# MAGIC

# COMMAND ----------

"""
create table partitioned by (transaction_date);

tablefolder
  transaction_date=2025-10-02 -- only 2025-10-02 data is present inside this folder
    - part-0
    - part-1
    - part-2
  transaction_date=2025-10-03
    - part-0
    - part-1
    - part-2
  transaction_date=2025-10-04
    - part-0
    - part-1
    - part-2
 
select * from customer_txn where transaction_date='2025-10-02';--This query will literally bring data from 2025-10-02 folder, without scanning any other folders.
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions customer_txn_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select * from customer_txn_part1 where transaction_date='2025-10-01'; --look at the partitionfilters in the below physical plan, it used the partitioning filtering (partition pruning)
# MAGIC --explain select * from customer_txn_part1 where txn_type='Online';--look at the partitionfilters in the below physical plan

# COMMAND ----------

#Just to show you how the data is partitioned in the filesystem (behind the scene)
spark.sql("select * from customer_txn").write.partitionBy("region").format("delta").save("/Volumes/lakehousecat1/deltadb/datalake/cust_txns_partdelta",mode='overwrite')

#equivalent CTAS in Pyspark python programming
#spark.sql("select * from customer_txn").write.partitionBy("region").saveAsTable("customer_txn_part2")

# COMMAND ----------

display(spark.sql('SHOW PARTITIONS customer_txn_part1'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM customer_txn_part1
# MAGIC WHERE transaction_date BETWEEN '2025-10-01' AND '2025-10-01';--picks the data from the 2025-10-01 folder directly and show the result quickly.

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Liquid Clustering
# MAGIC *Liquid Clustering is the* Next-generation data clustering feature that automatically manages physical data organization on disk to minimize scan cost for frequently queried columns only on Delta tables by performing automatic Optimize, Z-Ordering and Partitioning (kind of operation).<br>
# MAGIC
# MAGIC While clustering in databricks delta tables does partition happens literally?
# MAGIC No, liquid clustering does not create literal physical partitions (subdirectories). 
# MAGIC
# MAGIC Still we get the benifits of partitioning while doing clustering?
# MAGIC Yes, you absolutely still get the benefits of partitioning while doing clustering (where clustering will internally divide data for faster access)
# MAGIC
# MAGIC While clustering in databricks delta tables does partition happens literally or it will do partition kind of operation internally?
# MAGIC AI Response:
# MAGIC In Databricks Delta Tables, liquid clustering does not create physical partitions (i.e., it does not create subdirectories like /year=2024/month=01/ on your storage). Instead, it performs a "partition-like" operation internally by reorganizing data within files and using enhanced metadata in the Delta Log to optimize data skipping
# MAGIC
# MAGIC **Partition vs Liquid Clustering**
# MAGIC | Use case                       | Recommendation         |
# MAGIC | ------------------------------ | ---------------------- |
# MAGIC | High-cardinality columns       | Liquid clustering    |
# MAGIC | Frequently changing filters    | Liquid clustering    |
# MAGIC | Streaming / incremental loads  | Liquid clustering    |
# MAGIC | Static, low-cardinality (date) | Partition OR Liquid |
# MAGIC | Legacy Hive-style tables       | Partition           |
# MAGIC
# MAGIC
# MAGIC **Typical Use Cases**
# MAGIC - Large tables with frequent inserts, updates, and deletes.
# MAGIC - Query filtering on specific columns like customer_id, transaction_timestamp, order_date (high or low cardinal columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat1.deltadb

# COMMAND ----------

# MAGIC %md
# MAGIC ###Storage, Cost & Resource Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Vaccum
# MAGIC *VACUUM* in Delta Lake removes old, unused files to free up storage, default retention hours is 168. These files come from operations like DELETE, UPDATE, or MERGE and are kept temporarily so time-travel queries can work.<br>
# MAGIC
# MAGIC Before VACUUM<br>
# MAGIC Active + deleted parquet files exist<br>
# MAGIC
# MAGIC After VACUUM<br>
# MAGIC Only ACTIVE parquet files remains and delete Old parquet files (from UPDATE/MERGE/DELETE) after 1 week by default or we can increase or decrease the timeline<br>
# MAGIC Logs remain maintained (will not delete logs, only old data deleted)<br>
# MAGIC Time travel beyond retention hour becomes impossible (because data is literally deleted)<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM drugstbl_merge RETAIN 168 HOURS;
# MAGIC --SET spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The CLUSTER BY clause enables liquid clustering automatically.
# MAGIC CREATE TABLE IF NOT EXISTS sales_orders_liquid
# MAGIC (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   region STRING,
# MAGIC   product STRING,
# MAGIC   quantity INT,
# MAGIC   price DOUBLE,
# MAGIC   order_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (customer_id, region);--clustering column can be high or low cardinal, unlike partition which requires only low cardinal columns.
# MAGIC --column order used in cluster by is based on the primary filter, ie. whether you first filter based on customer_id or region, accordingly keep the coloumns order.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Each insert simulates separate data ingestion.
# MAGIC
# MAGIC INSERT INTO sales_orders_liquid VALUES
# MAGIC  (1, 101, 'North', 'Laptop', 2, 65000, '2025-10-01'),
# MAGIC  (2, 102, 'South', 'Headphones', 5, 2500, '2025-10-01'),
# MAGIC  (3, 103, 'West', 'Desk Chair', 3, 4500, '2025-10-02');
# MAGIC
# MAGIC INSERT INTO sales_orders_liquid VALUES
# MAGIC  (4, 101, 'North', 'Keyboard', 1, 1200, '2025-10-03'),
# MAGIC  (5, 104, 'East', 'Monitor', 2, 9500, '2025-10-03'),
# MAGIC  (6, 105, 'South', 'Mouse', 4, 700, '2025-10-03');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_orders_liquid where customer_id=102;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --check the clustering column
# MAGIC DESCRIBE detail sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sales_orders_liquid
# MAGIC SET price = price * 1.05
# MAGIC WHERE region = 'North';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_orders_liquid;--It proves the optimize and zordering is done naturally (look at the operationmetrics column numRemovedFiles: "3")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM sales_orders_liquid
# MAGIC WHERE region = 'East';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_orders_liquid;

# COMMAND ----------

#I am simulating the way how delta table liquid clustering working using spark (traditional) cluster by function... This is not liquid clustering of delta tables...
spark.sql("select * from lakehousecat1.deltadb.sales_orders_liquid order by region").coalesce(1).write.clusterBy("region").format("csv").save("/Volumes/lakehousecat1/deltadb/datalake/cust_txns_clustercsv",mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Delta Table – Copy Operation
# MAGIC
# MAGIC Delta Cloning allows to create a **copy of a Delta table** efficiently:
# MAGIC - **Full clone**: independent copy of data and metadata  
# MAGIC - **Shallow clone**: metadata-only copy referencing the same underlying data files  
# MAGIC
# MAGIC **Clone vs CTAS**
# MAGIC | Aspect                  | CLONE (Delta Lake)                     | CTAS (Create Table As Select)                |
# MAGIC | ----------------------- | -------------------------------------- | -------------------------------------------- |
# MAGIC | Type                    | Delta Lake feature                     | Standard SQL feature                         |
# MAGIC | Data copy               | Metadata-only (Shallow) or full (Deep) | Full physical data copy                      |
# MAGIC | Speed                   | Very fast (especially Shallow Clone)   | Slower for large tables                      |
# MAGIC | Storage usage           | Minimal for Shallow Clone              | High (duplicates data)                       |
# MAGIC | Time travel & history   | Preserved                              | Not preserved                                |
# MAGIC | Schema                  | Exact copy                             | Can be modified                              |
# MAGIC | Dependency on source    | Shallow clone depends on source files  | Fully independent                            |
# MAGIC | Use case                | Dev/Test copies, backups, experiments  | Aggregations, filtered or transformed tables |
# MAGIC | Source table type       | Delta tables only                      | Delta or non-Delta tables                    |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CTAS (Create Table as Select)
# MAGIC No properties of parent table, but only the output of the query is created as a new table dataset..
# MAGIC **Full copy** creates an **independent copy**:
# MAGIC - Table created based on the columns & datatype returned from the select query and Data (interally files) are alone **copied**
# MAGIC - **No other metadata (partition/clustered) will be copied**
# MAGIC - No historical deltalogs copied
# MAGIC - CTAS can be done with the subset rows or columns data copy, rather than copying all data (Benifit) 
# MAGIC - Eg. If I want to copy few columns and few rows from the parent table, CTAS or Insert select will help.

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat1.deltadb;
# MAGIC CREATE TABLE sales_orders_ctas AS SELECT * FROM sales_orders_liquid where product='Keyboard';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_orders_ctas;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_orders_ctas;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_orders_liquid

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Full/Deep Clone
# MAGIC
# MAGIC **Full clone** creates an **independent copy**:
# MAGIC - Data files are **copied**
# MAGIC - Medata copied
# MAGIC - No history is copied
# MAGIC - Uses more storage (because both data and metadata is copied into the new table)
# MAGIC - We have to clone the entire table without restricting rows or columns (we can't write query in clone)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE sales_orders_full_clone
# MAGIC CLONE sales_orders_liquid;
# MAGIC --CLONE select * from sales_orders_liquid where product='Keyboard'; --not possible

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_orders_full_clone

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_orders_full_clone

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Shallow Clone
# MAGIC
# MAGIC **Shallow clone** creates a **metadata-only copy (of the current version), no data copy**:
# MAGIC - Shares the same underlying data files (of the current version of the parent table)
# MAGIC - Very fast, uses minimal extra storage (only for metadata)
# MAGIC - A shallow clone shares data files, but it does NOT share the transaction log (maintained seperately)
# MAGIC - Even if two tables point to the same data files, they are logically independent because they have separate logs.
# MAGIC - If parent table is vacummed, then it affects shallow copied table also.
# MAGIC - If parent table modified, it will not affect child (shallow copied) table and vice versa.

# COMMAND ----------

# MAGIC %sql
# MAGIC --I am creating a deltalog to point data present in the delta file path of sales_orders_liquid
# MAGIC use lakehousecat.deltadb;
# MAGIC CREATE OR REPLACE TABLE sales_orders_shallow_clone
# MAGIC SHALLOW CLONE sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify shallow clone
# MAGIC SELECT count(1) FROM sales_orders_shallow_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_orders_shallow_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert into my source table will generate next version, which is not referred by the cloned table
# MAGIC INSERT INTO sales_orders_liquid VALUES
# MAGIC  (7, 101, 'North', 'Keyboard', 1, 1200, '2025-10-04');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sales_orders_liquid
# MAGIC SET price = 200.1
# MAGIC WHERE region = 'South';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Still points the old data files
# MAGIC SELECT * FROM sales_orders_shallow_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC --updating shallow copied table
# MAGIC UPDATE sales_orders_shallow_clone
# MAGIC SET price = 1000
# MAGIC WHERE region = 'North';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Still points the old data files (from source table) & updated data from (cloned table)
# MAGIC SELECT * FROM sales_orders_shallow_clone;

# COMMAND ----------

# MAGIC %md
# MAGIC ####7. Deletion Vector (otherwise called as SOFT DELETE feature)
# MAGIC A Deletion Vector is a metadata structure that marks specific rows as deleted inside a Parquet file, without rewriting the file.<br>
# MAGIC Eg. Instead of rewriting whole files, Delta just says: “row 3, row 15, row 102 are deleted”
# MAGIC DV Benifits:
# MAGIC - Parquet file count is unchanged, only dv log files are created (Soft Delete will happen)
# MAGIC - New DV files exist internally
# MAGIC
# MAGIC If you disable DV:
# MAGIC - File rewrite happens (Hard Delete will happen)
# MAGIC - New parquet files created
# MAGIC - Usecase is to ensure the deletion vector is set to True in the tables, to avoid real data deletion and file creation in the background, rather just mark the data as deleted in the DV file.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_dv AS
# MAGIC SELECT
# MAGIC   id AS order_id,
# MAGIC   CASE WHEN id % 2 = 0 THEN 'APAC' ELSE 'EMEA' END AS region
# MAGIC FROM range(0, 20);
# MAGIC select * from orders_dv;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --light weight operation of not really deleting the data, rather create a dv marker
# MAGIC ALTER TABLE orders_dv
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);--Enabling deletion vector will enable soft delete(it will not literally delete the data in the parquet file)

# COMMAND ----------

# MAGIC %sql
# MAGIC --look at the properties delta.enableDeletionVectors: "true"
# MAGIC DESCRIBE DETAIL orders_dv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_dv WHERE region = 'APAC';

# COMMAND ----------

# MAGIC %sql
# MAGIC --numDeletionVectorsAdded: "8" (no real deletes happened, just deletion vector added)
# MAGIC DESCRIBE HISTORY orders_dv

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_dv
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_dv WHERE order_id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_dv;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check the OperationMetrics (numRemovedFiles: "1", numRemovedBytes: "922", numCopiedRows: "9", numDeletionVectorsAdded: "0", numDeletionVectorsRemoved: "0")
# MAGIC DESCRIBE HISTORY orders_dv;
