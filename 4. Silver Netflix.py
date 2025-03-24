# Databricks notebook source
# MAGIC %md
# MAGIC ##Silver Transformation

# COMMAND ----------

df = spark.read.format("csv").option("header",True).option("inferSchema",True)\
    .load("abfss://bronze@sanetflixproject2309.dfs.core.windows.net/netflix_titles/")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = df.fillna({"duration_minutes":0,"duration_seasons":0})
df = df.withColumn("duration_minutes",df["duration_minutes"].cast(IntegerType()))

df = df.withColumn("ShortTitle",split(col("title"),":")[0]).withColumn("rating",split(col("rating"),"-")[0])

df.show(10)


# COMMAND ----------

df = df.withColumn("flag",when(col("type")=="Movie",1).when(col("type")=="TV Show",2).otherwise(0))
df.display(10)

# COMMAND ----------

from pyspark.sql.window import Window
df = df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))
df.display(10)

# COMMAND ----------

adf = df.groupBy("type").agg(count("*").alias("tcount"))
adf.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","abfss://silver@sanetflixproject2309.dfs.core.windows.net/netflix_titles/").save()