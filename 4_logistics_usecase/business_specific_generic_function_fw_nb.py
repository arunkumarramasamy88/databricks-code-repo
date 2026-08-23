# Databricks notebook source
# MAGIC %md
# MAGIC business specific (generic framework)

# COMMAND ----------

#staff data related functions
def staff_data_standardization(df):
    return df.withColumnRenamed('first_name', 'staff_first_name').withColumnRenamed('last_name', 'staff_last_name').withColumnRenamed('age', 'staff_age').withColumnRenamed('role', 'staff_role').withColumnRenamed('hub_location', 'staff_hub_location').withColumnRenamed('vehicle_type', 'staff_vehicle_type')

# COMMAND ----------

#shipment data related functions

def shipment_data_standardization(df):
    return df.withColumnRenamed('first_name', 'staff_first_name').withColumnRenamed('last_name', 'staff_last_name').withColumnRenamed('age', 'staff_age').withColumnRenamed('role', 'staff_role').withColumnRenamed('hub_location', 'staff_hub_location').withColumnRenamed('vehicle_type', 'staff_vehicle_type')
