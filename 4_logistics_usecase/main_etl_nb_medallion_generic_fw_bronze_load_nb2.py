# Databricks notebook source
# MAGIC %sql
# MAGIC select * from workspace.default.account

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Generic fw
# MAGIC 2. Medallion arch
# MAGIC 3. datalake
# MAGIC 4. lakehouse

# COMMAND ----------

# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_functions_nb1

# COMMAND ----------

#inline function
print("instantiating spark session")
from pyspark.sql.session import *
def return_sparksession(appname):
    return SparkSession.builder.appName(appname).getOrCreate()

# COMMAND ----------

#inline programming
#from pyspark.sql.session import *
#sparksess=SparkSession.builder.appName("logistics project").getOrCreate()
spark=return_sparksession("logistics project")

# COMMAND ----------

print("defining read function")
def return_df(sparksess,path,form='csv',delimiter=',',head=True,infsch=False,mulline=False):
    if form=='csv':
        df=sparksess.read.format(form).option("delimiter",delimiter).option("inferSchema",infsch).option("header",head).load(path)
    elif form=='json':
        df=sparksess.read.format(form).option("multiLine",mulline).load(path)
    elif form=='delta':
        df=sparksess.read.format(form).load(path)
    return df

# COMMAND ----------

#Development/learning/testing code - i am a production ready standard developer
form='csv'
delimiter=','
infsch=True
head=True
path='/Volumes/workspace/logistics/volume1/sourcedata/logistics_source1.txt'
mulline='true'
sparksess=spark
if form=='csv':
        df=sparksess.read.format(form).option("delimiter",delimiter).option("inferSchema",infsch).option("header",head).load(path)
elif form=='json':
        df=sparksess.read.format(form).option("multiLine",mulline).load(path)
elif form=='delta':
        df=sparksess.read.format(form).load(path)

source_df1=df

form='csv'
delimiter=','
infsch=True
head=True
path='/Volumes/workspace/logistics/volume1/sourcedata/logistics_source1.txt'
mulline='true'
sparksess=spark
if form=='csv':
        df=sparksess.read.format(form).option("delimiter",delimiter).option("inferSchema",infsch).option("header",head).load(path)
elif form=='json':
        df=sparksess.read.format(form).option("multiLine",mulline).load(path)
elif form=='delta':
        df=sparksess.read.format(form).load(path)

source_df2=return_df(spark,"/Volumes/workspace/logistics/volume1/sourcedata/logistics_source2.txt")

city_df3=return_df(spark,"/Volumes/workspace/logistics/volume1/sourcedata/Master_City_List.csv")

form='json'
delimiter=','
infsch=True
head=True
path='/Volumes/workspace/logistics/volume1/sourcedata/logistics_source1.txt'
mulline='true'
sparksess=spark
if form=='csv':
        df=sparksess.read.format(form).option("delimiter",delimiter).option("inferSchema",infsch).option("header",head).load(path)
elif form=='json':
        df=sparksess.read.format(form).option("multiLine",mulline).load(path)
elif form=='delta':
        df=sparksess.read.format(form).load(path)

shipment_df4=return_df(spark,"/Volumes/workspace/logistics/volume1/sourcedata/logistics_shipment_detail_3000.json",'json',mulline=True)
shipment_df4.show(2)

# COMMAND ----------

#Development/learning/testing code- i am not a production ready standard developer
write_df(source_df1,'/Volumes/workspace/logistics/volume1/datalaketarget/bronzetarget1','delta','append')#datalake
write_df(source_df2,'/Volumes/workspace/logistics/volume1/datalaketarget/bronzetarget2','delta','overwrite')
write_df(city_df3,'/Volumes/workspace/logistics/volume1/datalaketarget/bronzecity3','delta','overwrite')
#write_df(city_df3,'workspace.default.city_lat_long','tbl')#datalake -> SQL Layer -> Lakehouse
