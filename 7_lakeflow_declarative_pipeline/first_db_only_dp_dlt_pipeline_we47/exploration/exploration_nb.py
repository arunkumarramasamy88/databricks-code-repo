# Databricks notebook source
# MAGIC %sql
# MAGIC select * from lakehousecat.deltadb.drugstbl_gold_view2

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from workspace.default.silver_staff_tbl1;
# MAGIC --insert into workspace.default.silver_staff_tbl1 (shipment_id,staff_first_name) values(6000203,'irfan');
# MAGIC --insert into workspace.default.silver_staff_tbl1 (shipment_id,staff_first_name) values(6000210,'irfan');
# MAGIC update workspace.default.silver_staff_tbl1 set staff_first_name='mohamed' where shipment_id=6000203

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog1_we47.schema1_we47.shipment1_bronze2

# COMMAND ----------

#from pyspark import pipelines as dp
#@dp.table()
def return_df():
    df1=spark.read.table("gcp_mysql_fc_wd361.logistics.shipments1").where("updated_at > '2026-02-09'")
    return df1
return_df().write.saveAsTable("catalog1_we47.schema1_we47.shipmentwe47123",mode='overwrite')
#display(return_df())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog1_we47.schema1_we47.shipmentwe47123;
