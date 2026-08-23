# Databricks notebook source
# MAGIC %md
# MAGIC ###1. Custom API Ingestion
# MAGIC API → Raw JSON files (Custom Ingestion)  -> Load to Datalake/Cloud Buckets -> Auto Loader reads API files incrementally -> Incremental load from Datalake to Bronze
# MAGIC
# MAGIC **Story:**
# MAGIC I implemented Lakeflow custom ingestion by pulling data from a 3rd party (Inceptezlabs/yext) REST API service (offers the driver registration information) using Python, landing raw JSON files into Datalake/Cloud storage, and using Auto Loader with schema evolution to incrementally ingest and normalize(structurize) data into a Bronze Delta table/File (as it is)
# MAGIC
# MAGIC ###2. Auto Loader is Databricks
# MAGIC **Auto Loader is Databricks’ cloud-native file ingestion engine for ingesting new files incrementally from object storage.**
# MAGIC
# MAGIC Supported Sources:
# MAGIC - AWS S3
# MAGIC - Azure ADLS Gen2
# MAGIC - Google Cloud Storage (GCS)
# MAGIC
# MAGIC Modes:
# MAGIC - Directory listing - Directory listing scans storage paths to detect new files
# MAGIC - File notification - Processes files as soon as they arrive at scale
# MAGIC
# MAGIC **Directory listing**
# MAGIC 1. Spark lists directory (pull model)
# MAGIC 2. Detects new CSV file
# MAGIC 3. Infers schema / evolves if needed
# MAGIC 4. Processes the file
# MAGIC 5. Updates checkpoint (file1 is processed...)
# MAGIC 6. Waits for next trigger
# MAGIC
# MAGIC **File Notification**
# MAGIC 1. Cloud storage emits file-create event (S3 Event, ADLS Event Grid, GCS Pub/Sub)
# MAGIC 2. Event is delivered to Databricks queue
# MAGIC 3. Auto Loader receives notification (push model)
# MAGIC 4. New file is registered
# MAGIC 5. Schema is inferred / evolved if needed
# MAGIC 6. File is processed immediately
# MAGIC 7. Checkpoint is updated
# MAGIC 8. Stream stays idle until next event arrives

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/6_lakeflow_pipelines/api_autoloader.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Custom Ingesting data from REST API to Datalake (DBFS/S3)

# COMMAND ----------

#We didn't used Spark at all in this cell
import requests
import json
from datetime import datetime

url = "http://inceptezlabs.com/api.php"

'''
#Short version of our below code
resp = requests.get(url) #get the raw json(dictionary) data from api
data = resp.json() #raw json data in python dictionary format we received
proper_jsondata=json.dumps(data) #convert the python dictionary to json string to keep in our datalake for everyone's usage
ts = datetime.now().strftime("%Y%m%d%H%M%S")
output_path = f"/Volumes/lakehousecat1/deltadb/datalake/apidata/posts_{ts}.json"
dbutils.fs.put(output_path,proper_jsondata,overwrite=True)
'''

def fetch_and_save_data():#inline function to pull data from REST API to datalake(Cloud storage)
    try:        
        resp = requests.get(url)

        if resp.status_code != 200:
            print(f"Failed to fetch data. Status Code: {resp.status_code}")
            print(f"Response: {resp.text}")
            return

        try:
            data = resp.json()
            print("Data received successfully:")
            print(data)
        except json.JSONDecodeError:
            print("Error: The response is not valid JSON.")
            return

        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"/Volumes/lakehousecat1/deltadb/datalake/apidata/posts_{ts}.json"#assume as a cloud storage/dbfs datalake
        
        try:
            dbutils.fs.put(
                output_path,
                json.dumps(data),
                overwrite=True
            )
            print(f"Successfully wrote to {output_path}")
        except NameError:
            print("Error: 'dbutils' is not defined. This code must run in a Databricks notebook.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    fetch_and_save_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Auto loader from Datalake to Bronze Layer (Datalake & Lakehouse)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
user_schema = StructType([
    StructField("uid", StringType()),
    StructField("user", StructType([
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("location", StringType()),
        StructField("registered", StringType()),])),])
#spark.read.schema(someschema).json(location) #if the data in the location is simple json
#spark.read.json(location) #if the data in the location is nested json, we apply schema at columns levels later using from_json(column,schema)

df_raw = (spark.readStream
        .format("cloudFiles")#.schema(user_schema)
        .option("cloudFiles.format", "json")
        #.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation","/Volumes/catalog3_we47/schema3_we47/datalake/bronze/streamwrite41/_schema")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .load("/Volumes/lakehousecat1/deltadb/datalake/apidata/"))
#df_raw will contain a row like below...
#status     data(string datatype contains json data, we cant access using dot notation)
#success    {"uid": "7e168858-cb80-46a6-a480-7b2a54ca61d9","user": {"name": "Afsheen Williams","email": "afsheen@example.com","location": "Dubai","registered": "2026-02-07T07:05:27+00:00"}}

#Making the data in a semi structured format using from_json
parsed_df = df_raw.withColumn("data", F.from_json(F.col("data"), user_schema))#important function from_json to convert json string data in the column to the custom schema applied json data in a hierarchical fashion like data.uid or data.user.email...
#Error: Can't extract a value from "data". Need a complex type [STRUCT, ARRAY, MAP] but got "STRING". SQLSTATE: 42000

#Structurize the semi structured hierarchical json data to delimited structure format
df_user = (
    parsed_df
        .select(
            F.col("data.uid").alias("uid"),
            F.col("data.user.name").alias("user_name"),
            F.col("data.user.email").alias("user_email"),
            F.col("data.user.location").alias("user_location"),
            F.to_timestamp(
                F.col("data.user.registered")
            ).alias("user_registered_ts"),
            F.current_timestamp().alias("ingestion_ts")
        )
)

(
    df_user.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            "/Volumes/catalog3_we47/schema3_we47/datalake/bronze/streamwrite4/_checkpoint")
        .start(
            "/Volumes/catalog3_we47/schema3_we47/datalake/bronze/streamwrite45"
        )
)

(df_user.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            "/Volumes/catalog3_we47/schema3_we47/datalake/bronze/streamwrite41/_checkpoint"
        )
        .toTable("catalog3_we47.schema3_we47.bronze_user_api2"))


# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Validate the data

# COMMAND ----------

display(spark.sql("select * from catalog3_we47.schema3_we47.bronze_user_api2 order by user_registered_ts desc"))

# COMMAND ----------

display(spark.read.format("delta").load("/Volumes/catalog3_we47/schema3_we47/datalake/bronze/streamwrite45").orderBy("user_registered_ts", ascending=False))
