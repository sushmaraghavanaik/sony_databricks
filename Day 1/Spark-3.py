# Databricks notebook source
##Using Pyspark

df_sales = spark.read.csv("/Volumes/sony_databricks_workspace/default/raw/sales.csv", header=True, inferSchema=True)
df_sales.display()

df.order_dates = spark.read.csv("/Volumes/sony_databricks_workspace/default/raw/order_dates.csv")
df.order_dates .display()

df_products = spark.read.json("/Volumes/sony_databricks_workspace/default/raw/products.json")
df_products.display()

df_customers = spark.read.json("/Volumes/sony_databricks_workspace/default/raw/customers.json")
df_customers.display()



# COMMAND ----------

##Save as a table 

df_customers.write.saveAsTable("customer")
df_sales.write.saveAsTable("sales")


## TO overwrite
##This will only work if schema is same but data is different
df_products.write.mode("overwrite").saveAsTable("customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table customers

# COMMAND ----------

# MAGIC %run "/Workspace/Users/sushmaraghavanaik@gmail.com/Day 1/includes"
# MAGIC

# COMMAND ----------

df_sales = spark.read.csv(f"{input_path}/sales.csv", header=True, inferSchema=True)
df_sales_new = add_ingestion(df_sales)
df_sales_new.write.saveAsTable("sales")

# COMMAND ----------

df_sales_new.display()

# COMMAND ----------

df_customers = spark.read.json(f"{input_path}/customers.json")
df_customers_new = add_ingestion(df_customers)
df_customers_new.write.saveAsTable("customers")

# COMMAND ----------

df_products = spark.read.json(f"{input_path}/products.json")
df_products_new = add_ingestion(df_products)
df_products_new.write.saveAsTable("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Using Spark SQL
# MAGIC
# MAGIC -- Syntax to ingest from files
# MAGIC -- select * from file_format.`input_path`;
# MAGIC
# MAGIC CREATE table customers_Spark_SQL as 
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/sony_databricks_workspace/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table products_Spark_SQL as 
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/sony_databricks_workspace/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table sales_Spark_SQL as 
# MAGIC select *, current_timestamp() as ingestion_date from csv.`/Volumes/sony_databricks_workspace/default/raw/sales.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table order_dates_Spark_SQL as 
# MAGIC select *, current_timestamp() as ingestion_date from csv.`/Volumes/sony_databricks_workspace/default/raw/order_dates.csv`
