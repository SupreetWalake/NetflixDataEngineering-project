# Databricks notebook source
# MAGIC %md
# MAGIC #Silver Notebook
# MAGIC ###Array Parameter

# COMMAND ----------

files = [
    {
        "source_folder":"netflix_directors",
        "target_folder":"netflix_directors"
    },
    {
        "source_folder":"netflix_countries",
        "target_folder":"netflix_countries"
    },
    {
        "source_folder":"netflix_category",
        "target_folder":"netflix_category"
    },
    {
        "source_folder":"netflix_cast",
        "target_folder":"netflix_cast"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="my_arr",value=files)