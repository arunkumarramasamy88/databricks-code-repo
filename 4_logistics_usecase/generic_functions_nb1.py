# Databricks notebook source
# MAGIC %md
# MAGIC This generic func nb is developed by Framework Developers (Data engineers)

# COMMAND ----------

#Generic () Functions
print("instantiating spark session")
from pyspark.sql.session import *
def return_sparksession(appname):
    return SparkSession.builder.appName(appname).getOrCreate()

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

print("defining writing function")
def write_df(df,tgt,tgtformat=None,mo='overwrite'):
    if tgtformat == 'csv':
        df.write.mode(mo).format(tgtformat).option("delimiter",",").option("header",True).save(tgt)
    elif tgtformat == 'json':
        df.write.mode(mo).format(tgtformat).save(tgt)
    elif tgtformat == 'delta':
        df.write.mode(mo).format(tgtformat).save(tgt)
    elif tgtformat=='tbl':
        df.write.mode(mo).saveAsTable(tgt)
    else:
        print("Invalid target format -  for now our fw supports csv,json,delta,table")

# COMMAND ----------

#ETL generic functions
def staff_munging(df,opt,cols):
    return df.na.drop(how=opt,subset=cols)
