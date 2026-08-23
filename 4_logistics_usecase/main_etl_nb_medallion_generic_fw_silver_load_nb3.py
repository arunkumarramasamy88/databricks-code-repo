# Databricks notebook source
# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_functions_nb1

# COMMAND ----------

# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/business_specific_generic_function_fw_nb

# COMMAND ----------

#spark=return_sparksession("logistics project")
bronze_df1=return_df(spark,"/Volumes/workspace/logistics/volume1/datalaketarget/bronzetarget1",'delta')
bronze_df1.show(2)

# COMMAND ----------

silver_df1=staff_data_standardization(bronze_df1)

# COMMAND ----------

#write_df(silver_df1,"silver_staff_tbl1",'tbl')
spark.read.table("silver_staff_tbl1").show(10)
