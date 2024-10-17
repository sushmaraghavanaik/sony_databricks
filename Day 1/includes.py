# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path = "/Volumes/sony_databricks_workspace/default/raw"

# COMMAND ----------

def add_ingestion(df):
    df1 = df.withColumn("ingestion_timestamp", current_timestamp())
    return df1
