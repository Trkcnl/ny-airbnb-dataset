# Databricks notebook source
data_path = "file:/Workspace/Users/fmc42@bath.ac.uk/ny-airbnb-dataset/data/"

df_listings = spark.read.option("header",'True').option('delimiter', ',').option("inferSchema",'True').csv(data_path+"listings.csv")
df_calendar = spark.read.option("header",'True').option('delimiter', ',').option("inferSchema",'True').csv(data_path+"calendar.csv")
df_reviews = spark.read.option("header",'True').option('delimiter', ',').option("inferSchema",'True').csv(data_path+"reviews.csv")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_calendar_cleaned=df_calendar.withColumn('price', regexp_replace('price', '\$', ''))
df_calendar_cleaned=df_calendar_cleaned.withColumnRenamed('price', 'price($)')
df_calendar_cleaned=df_calendar_cleaned.withColumn('price($)', df_calendar_cleaned["price($)"].cast(FloatType()))
display(df_calendar_cleaned)

