# Databricks notebook source
# MAGIC %md
# MAGIC ###Widgets utility used for adding the components/widgets into our notebook for creating
# MAGIC dynamic/parameterized approaches

# COMMAND ----------

print("can you create a textbox widget")
dbutils.widgets.text("tablename","cities","enter the tablename to query")
dbutils.widgets.text("filtercond","id=1","enter the tablename to query")

# COMMAND ----------

print("can you get the value of the widget using dbutils.widgets.get and store into a local python variable tblname")
tblname=dbutils.widgets.get("tablename")
filter1=dbutils.widgets.get("filtercond")
print("user passed the value of ?",tblname)

# COMMAND ----------

display(spark.sql(f"select * from default.{tblname} where {filter1} limit 10"))
#spark.read.table(tblname)

# COMMAND ----------



# COMMAND ----------

dbutils.help()
#credentials/secrets
#fs
#jobs
#notebook
#widgets

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

print("copying")
dbutils.fs.cp("dbfs:///Volumes/workspace/default/volumewd36/sample_healthcare_patients.csv","/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv")
print("head of 10 rows")
dbutils.fs.head("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv")
print("listing")
dbutils.fs.ls("/Volumes/workspace/default/volumewd36/")
print("make directory")
dbutils.fs.mkdirs("/Volumes/workspace/default/volumewd36/healthcare/")
print("move")
dbutils.fs.mv("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv","/Volumes/workspace/default/volumewd36/healthcare/sample_healthcare_patients1.csv")
dbutils.fs.ls("/Volumes/workspace/default/volumewd36/healthcare/")
dbutils.fs.cp("/Volumes/workspace/default/volumewd36/sample_healthcare_patients.csv","/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv")
print("put to write some data into a file")

# COMMAND ----------

print("try without the 3rd argument of true, you will find the dbfs-> hadoop -> spark -> s3 bucket")
#dbutils.fs.put("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv","put some content",False)
print(dbutils.fs.head("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv"))
dbutils.fs.put("dbfs:///Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv","put something",True)
print("see the data in the file")
dbutils.fs.head("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook"

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook",100)
# I want to run particular cell from the notebook instead execute entire notebook using dbutils.notebook.runs. Can u do that?
