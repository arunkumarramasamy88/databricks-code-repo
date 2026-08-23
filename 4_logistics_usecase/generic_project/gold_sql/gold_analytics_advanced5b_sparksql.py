# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

import json

config_nb_output = dbutils.notebook.run(
    "/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/configs_path1",
    120,{"catalog": CATALOG,"schema": SCHEMA})

config_dict = json.loads(config_nb_output)

GOLDDB = config_dict["GOLDDB"]

# COMMAND ----------

# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/util_functions2

# COMMAND ----------

#We are building a Lakehouse Advanced Analytics table
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLDDB}.gold_top3_drivers_tbl
USING DELTA
AS
SELECT *
FROM (
    SELECT *,
           DENSE_RANK() OVER (
               PARTITION BY origin_hub_city
               ORDER BY shipment_cost DESC
           ) AS rank
    FROM {GOLDDB}.gold_core_curated_tbl
)
WHERE rank <= 3
""")


