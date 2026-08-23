# Databricks notebook source
dbutils.widgets.text("catalog","")
dbutils.widgets.text("schema","")
catalog=dbutils.widgets.get("catalog")
schema=dbutils.widgets.get("schema")
print(catalog)
print(schema)
#catalog='inceptezcatalog'
#schema='schema1'

# COMMAND ----------


SRC1=f'/Volumes/{catalog}/{schema}/bronze'
TGT1='/Volumes/{catalog}/{schema}/silver'

# COMMAND ----------

dbutils.notebook.exit({
  "src": SRC1,
  "tgt": TGT1})
