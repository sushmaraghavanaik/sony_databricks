# Databricks notebook source
data = [(1,'a',20), (2, 'b', 30)]
schema = ["id", "name", "age"]
## OR schema = "id int, name string, age int"
df=spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------

# DataFrame functions - no need to import (always used with .)
# Example .select, .withColumnRenamed

# Functions - need to import
# Example col()

##Only transformation
df.select("*") 

##Transformation and action
df.select("*").display()
df.select("id", "name").display()

## DO NOT SAVE LIKE THIS because we need to save a Transformation and not Action
wrong_df1 = df.select("*").display()

#Saving the Transformation
df1 = df.select("*")
df1.display()

# COMMAND ----------

##Change the column name using alias while printing

from pyspark.sql.functions import *
df.select(col("id").alias("emp_id")).display()

df.display()

df.withColumnRenamed("id", "emp_id").display()

df.display()

df.withColumnsRenamed({"id": "emp_id", "name": "emp_name", "age": "emp_age"}).display()

df.display()

## TO save these name change in column names , save it

dfwithColumnsRenamed = df.withColumnsRenamed({"id": "emp_id", "name": "emp_name", "age": "emp_age"})

dfwithColumnsRenamed.display()

# COMMAND ----------

##HELP with examples

help(df.withColumnRenamed)

# COMMAND ----------

## Add a new column or replace existing column only while displaying, to save you need to save it into the same or different dataframe

df_temp= df

df_temp.withColumn("current_date", current_date()).display()

df_temp.display()

df_temp.withColumn("age", current_date()).display()

df_temp.display()


