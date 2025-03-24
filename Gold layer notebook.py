# Databricks notebook source
# MAGIC %md
# MAGIC ##Gold Layer

# COMMAND ----------

rules ={
    "rule1" :"show_id is NOT NULL"
}

# COMMAND ----------

va_ar = ["netflix_cast","netflix_category","netflix_countries","netflix_directors"]

for i in va_ar:
    cv=f"gold_{i}"
    @dlt.table(name = cv)
    @dlt.expect_all_or_drop(rules)
    def gold_table():
        df = spark.readStream.format("delta").load(f"abfss://silver@sanetflixproject2309.dfs.core.windows.net/{i}")
        return df
    