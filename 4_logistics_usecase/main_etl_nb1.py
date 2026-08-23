# Databricks notebook source
from pyspark.sql.session import *
sparksess=SparkSession.builder.appName("logistics project").getOrCreate()

# COMMAND ----------

#Development/learning/testing code - i am not a production ready standard developer
source_df1=spark.read.csv("/Volumes/workspace/logistics/volume1/sourcedata/logistics_source1",sep=',',inferSchema=False,header=True)
source_df2=spark.read.csv("/Volumes/workspace/logistics/volume1/sourcedata/logistics_source2",sep=',',inferSchema=False,header=True)
city_df3=spark.read.csv("/Volumes/workspace/logistics/volume1/sourcedata/Master_City_List.csv",sep=',',inferSchema=False,header=True)

# COMMAND ----------

#Development/learning/testing code
source_df1.write.format("delta").save("/Volumes/workspace/logistics/volume1/datalaketarget/bronzesource1",mode='overwrite')
source_df2.write.format("delta").save("/Volumes/workspace/logistics/volume1/datalaketarget/bronzesource2",mode='overwrite')
city_df3.write.format("delta").save("/Volumes/workspace/logistics/volume1/datalaketarget/bronzecity3",mode='overwrite')


# COMMAND ----------

#Development/learning/testing code - i am a production ready standard developer

