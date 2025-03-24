# Databricks notebook source
# MAGIC %md
# MAGIC ##Silver 
# MAGIC ==Notebook Lookup Table

# COMMAND ----------

dbutils.widgets.text("source folder","netflix_directors")
dbutils.widgets.text("target folder","netflix_directors")

# COMMAND ----------

src_folder=dbutils.widgets.get("source folder")
tgt_folder=dbutils.widgets.get("target folder")

# COMMAND ----------

df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(f"abfss://bronze@sanetflixproject2309.dfs.core.windows.net/{src_folder}")

# COMMAND ----------

df.write.format("delta").mode("append").option("path",f"abfss://silver@sanetflixproject2309.dfs.core.windows.net/{tgt_folder}").save()