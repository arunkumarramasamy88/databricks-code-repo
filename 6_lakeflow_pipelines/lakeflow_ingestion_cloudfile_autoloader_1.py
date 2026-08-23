# Databricks notebook source
# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/6_lakeflow_pipelines/autoloader_file_ingestion_usecase1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Auto Loader is Databricks
# MAGIC **Auto Loader is Databricks’ cloud-native file ingestion engine for ingesting new files incrementally from object storage.**
# MAGIC
# MAGIC Supported Sources:
# MAGIC - AWS S3
# MAGIC - Azure ADLS Gen2
# MAGIC - Google Cloud Storage (GCS)
# MAGIC
# MAGIC Modes:
# MAGIC - **Directory listing** - Directory listing scans storage paths to detect new files (This works in free edition)
# MAGIC - File notification - Processes files as soon as they arrive at scale (This will not work in free edition because the cloud storage event trigger can't control/trigger Databricks LF Ingestion)
# MAGIC
# MAGIC **Directory listing** (Databricks Lakeflow Ingestion - Autoloader - Directory Listing)
# MAGIC 1. Spark lists the Cloud directory (pull model)
# MAGIC 2. Detects new files **(Incremental Autoloader)**
# MAGIC 3. Infers schema / **evolves** if needed
# MAGIC 4. Copy the file(s) & store the schema info in a schema file, so further schema inference is not needed.
# MAGIC 5. After file1 is copied to Bronze layer -> Updates checkpoint (maintaining the file info of whichever is copied already)
# MAGIC 6. Waits for next trigger of the Lakeflow pipeline and follow step 1 to 5.
# MAGIC
# MAGIC **File Notification** (we will see it in the cloud databricks version)
# MAGIC 1. Cloud storage emits file-create event (S3 Event, ADLS Event Grid, GCS Pub/Sub)
# MAGIC 2. Event is delivered to Databricks queue
# MAGIC 3. Auto Loader receives notification (push model)
# MAGIC 4. New file is registered
# MAGIC 5. Infers schema / evolves if needed
# MAGIC 6. Copy the file(s) & store the schema info in a schema file, so further schema inference is not needed.
# MAGIC 7. Updates checkpoint (file1 is processed...)
# MAGIC 8. Stream stays idle until next event arrives

# COMMAND ----------

# MAGIC %md
# MAGIC **Benifits of Autoloader:**
# MAGIC - Incremental and Efficient File Ingestion: Auto Loader automatically detects and processes new files as they arrive in your source directory (e.g., S3 or Unity Catalog volume). This eliminates manual tracking and reprocessing, ensuring only new data is ingested each run.
# MAGIC
# MAGIC - Schema Evolution Support: With options like "cloudFiles.schemaEvolutionMode": "addNewColumns" and "mergeSchema": "true", Auto Loader can handle changes in your data schema over time, adding new columns without breaking your pipeline.
# MAGIC
# MAGIC - Scalability and Resource Optimization: Properties such as "cloudFiles.maxFilesPerTrigger" allow you to control how many files are processed per batch, helping manage resource usage and scale to large datasets.
# MAGIC
# MAGIC - Checkpointing and Fault Tolerance: Auto Loader maintains checkpoints and schema locations, so it can resume from where it left off in case of failures, ensuring reliable and consistent data ingestion.
# MAGIC
# MAGIC - Unified Streaming and Batch Processing: By using readStream and writeStream, your pipeline can handle both streaming and batch workloads seamlessly, making it suitable for real-time and scheduled data ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC **To perform schema evolution, we have to use the below properties:**<br>
# MAGIC **Read side:** <br>
# MAGIC .option("cloudFiles.schemaEvolutionMode","addNewColumns")<br>
# MAGIC **Write side:** <br>
# MAGIC .option("mergeSchema", "true")<br>

# COMMAND ----------

#We learn Autoloading of Incremental data from cloud source, Schema evolution, 
cloudsrc="/Volumes/catalog1_we47/schema1_we47/clouddatalake/sourcesystemdata/"#s3 storage path
#cloudsrc="gs://izsourcebucket/Master_City_List_hour1.csv"
bronzetgt="/Volumes/catalog3_we47/schema3_we47/datalake/bronze/ourtargetlocation/"
#To resolve it, you must use a data source that is accessible from your AWS-based Databricks workspace, such as an S3 bucket or a Unity Catalog volume.
ckptlocation="/Volumes/catalog1_we47/schema1_we47/clouddatalake/ckpt/_checkpoint"#stores the files copied information post write is successful
schemalocation="/Volumes/catalog1_we47/schema1_we47/clouddatalake/_schema"#stores the inferred schema of the source data
df1=spark.readStream.format("cloudFiles")\
.option("cloudFiles.format","csv")\
.option("cloudFiles.maxFilesPerTrigger",1)\
.option("cloudFiles.inferColumnTypes",True)\
.option("cloudFiles.schemaEvolutionMode","addNewColumns")\
.option("checkpointLocation", ckptlocation)\
.option("cloudFiles.schemaLocation", schemalocation)\
.option("header",True)\
.load(cloudsrc)#this can be s3/adls/gcs
#.option("cloudFiles.useNotifications", "true") (Remove this option to enable directory listing)
#maxFilesPerTrigger - this property help spark to process howmany files in an iteration to control the resource utilization (all files will be processed ultimately)

# COMMAND ----------

# DBTITLE 1,Cell 4
#realtime trigger is not possible in free serverless
#writeStream will read data from df1 (materialized here) and write to bronzetgt using the schema generated by reader and checkpoint info stored
df1.writeStream.trigger(availableNow=True)\
.option("checkpointLocation", ckptlocation)\
.option("cloudFiles.schemaLocation", schemalocation)\
.option("mergeSchema", "true") \
.start(bronzetgt)
#.option("mergeSchema", "true") \

# COMMAND ----------

spark.read.format("delta").load(bronzetgt).orderBy("city_name").show(100)
