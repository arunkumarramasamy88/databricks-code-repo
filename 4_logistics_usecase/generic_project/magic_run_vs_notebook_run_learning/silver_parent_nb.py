# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog")
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema")
print(CATALOG)
print(SCHEMA)

# COMMAND ----------

#Benfit: pass parameters
#Drawback: We can't run the notebook inline/locally, so we can't take the output of the child notebook directly
child_nb_output=dbutils.notebook.run("/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/5_all_databricks_workouts/child1_nb_config",60,{"catalog":CATALOG,"schema":SCHEMA})
print(child_nb_output)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Drawback: Can't pass parameters
# MAGIC ######Benifi: We can run the child notebook inline/locally, so we can take the output of the child notebook directly

# COMMAND ----------

# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/5_all_databricks_workouts/child1_nb_config

# COMMAND ----------

# MAGIC %run ./child2_nb_generic_function

# COMMAND ----------

print(SRC1)
print(TGT1)

# COMMAND ----------

print(fun1(100))
