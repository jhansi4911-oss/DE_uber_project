# Databricks notebook source
files =[
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"bulk_rides"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:
    path =f"abfss://raw@dluberproject1.dfs.core.windows.net/ingestion/{file['file']}.json"

    df = spark.read.format("json").option("multiLine", "true").load(path)
    df.write.format('delta').mode("overwrite").option("mergeSchema", "true").saveAsTable(f"uber_project.bronze.{file['file']}")



# COMMAND ----------

import pandas as pd

# COMMAND ----------

url = "abfss://raw@dluberproject1.dfs.core.windows.net/ingestion/bulk_rides.json"

df = spark.read.json(url)
df_spark = df
if not spark.catalog.tableExists("uber_project.bronze.bulk_rides"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(f"uber_project.bronze.bulk_rides")
    print("This will not run more than 1 time")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from uber_project.bronze.map_cities

# COMMAND ----------

# DBTITLE 1,Cell 4
df = spark.read.format("json").option("multiLine", "true").load(path)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from uber_project.bronze.bulk_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from uber_project.bronze.rides_raw