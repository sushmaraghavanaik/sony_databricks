# Databricks notebook source
df_sales = spark.table("sales")

df_customer=spark.table("customers")

df_joined = df_sales.join(df_customer, df_sales["customer_id"]==df_customer["customer_id"],"inner")

df_joined.display()

# COMMAND ----------



# COMMAND ----------

df_customer.filter("customer_id==2").display()

# TO show ythe details about pyspark jobs and stages
#Yo can also see this by accessing Spark UI using the view link in the Soark jobs drop down
df_customer.filter("customer_id==2").explain()




# COMMAND ----------


from pyspark.sql.functions import *
df_customer.where(col('customer_id')==2).display()


# COMMAND ----------

df_customer.sort("customer_city").display()

#Not recommended
df_customer.sort("customer_city", ascending = True).display()

##Recommended
df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined = df_customer.join(df_sales, "customer_id", "inner")

# COMMAND ----------

## .count() acts as transformation as well as action based on how it is used


df_joined.groupBy("customer_id").count()

df_joined.groupBy("customer_id").count().orderBy("customer_id").display()

# COMMAND ----------

## In Spark SQL
## Not ideal method

spark.sql("select count(*) from sales group by 'customer_id'").show()


spark.sql("select count(*) from sales group by customer_id order by customer_id").show()



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1),customer_id from
# MAGIC customers
# MAGIC group by
# MAGIC customer_id
# MAGIC order by
# MAGIC customer_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers
