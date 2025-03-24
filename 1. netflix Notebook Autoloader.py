# Databricks notebook source
# MAGIC %md
# MAGIC ##1.Autoloader
# MAGIC --Incremental data loading using Autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema netflix_catalog.net_schema

# COMMAND ----------

checkpoint_location = "abfss://silver@sanetflixproject2309.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation",checkpoint_location)\
    .load("abfss://raw@sanetflixproject2309.dfs.core.windows.net")

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream.option("checkpointLocation",checkpoint_location).trigger(processingTime="10 seconds")\
    .start("abfss://bronze@sanetflixproject2309.dfs.core.windows.net/netflix_titles1")