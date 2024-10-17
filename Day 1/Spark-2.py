# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
import datetime

users=[
    {
        "id":1,
        "name":"Sachin",
        "last_name":"Tendulkar",
        "email":"sachin@gmail.com",
        "Mobile":Row(mobile= "342779900", home= "91568557700"),
        "courses": [1,2],
        "is_customer":True,
        "DOB": datetime.date(1973,4,24)
    },
    {
         "id":2,
        "name":"Virat",
        "last_name":"Kohli",
        "email":"virat@gmail.com",
        "Mobile":Row(mobile= "91556600", home= "918912300"),
        "courses": [2,3],
        "is_customer":True, 
        "DOB":datetime.date(1988,11,5)
    },
     {
         "id":3,
        "name":"Rohit",
        "last_name":"Sharma",
        "email":"rohit@gmail.com",
        "Mobile":Row(mobile= "914455700", home= "9145997700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1987,4,30)
     },
     {
         "id":4,
        "name":"Dinesh",
        "last_name":"Karthik",
        "email":"dinesh@gmail.com",
        "Mobile":Row(mobile= "91467700", home= "916789700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1985,6,1)
     },
     {
         "id":5,
        "name":"M S",
        "last_name":"Dhoni",
        "email":"dhoni@gmail.com",
        "Mobile":Row(mobile= "91467799", home= "916778800"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1981,7,7)
     }
]

df=spark.createDataFrame(users)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Task:
# MAGIC
# MAGIC 1. Rename id col to emp_id
# MAGIC 2. to new col with current timestamp
# MAGIC 3. drop is_customer col
# MAGIC 4. concat name and last_name
# MAGIC     
# MAGIC     

# COMMAND ----------

df = df.withColumnRenamed("id", "emp_id")

df.display()

# COMMAND ----------

df = df.withColumn("Current_Date", current_timestamp())

df.display()

# COMMAND ----------

df = df.drop("is_customer")

df.display()

# COMMAND ----------

df = df.select("*", concat(col("name"), lit(" "), col("last_name")))

df.display()


##OR 

df = df.withColumn("full_name", concat("name", lit(" "), "last_name"))

df.display()

# COMMAND ----------

## OR ALL Together in same code

df_users_final = df\
.withColumnRenamed("id", "emp_id")\
.withColumn("current_timestamp", current_timestamp())\
.withColumn("full_name", concat("name", lit(" "), "last_name"))\
.drop("is_customer", "name", "last_name")

df_users_final.display()

# COMMAND ----------

# Extracting data from arrays and objects
#From mobile column and course column (array)

df_users_final\
.withColumn("mobile_office", col("Mobile.mobile"))\
.withColumn("mobile_home", col("Mobile.home"))\
.drop("mobile")\
.withColumn("courses", explode("courses"))\
.display()



# COMMAND ----------

# MAGIC %md
# MAGIC #ASSIGNMENT
# MAGIC ####Link: https://opensource.adobe.com/Spry/samples/data_region/JSONDataSetSample.html
# MAGIC
# MAGIC ####Create Data frame and structure it, distribute all the arrays, objects
# MAGIC ####Example 4/ Example 5
# MAGIC
# MAGIC {
# MAGIC 	"id": "0001",
# MAGIC 	"type": "donut",
# MAGIC 	"name": "Cake",
# MAGIC 	"ppu": 0.55,
# MAGIC 	"batters":
# MAGIC 		{
# MAGIC 			"batter":
# MAGIC 				[
# MAGIC 					{ "id": "1001", "type": "Regular" },
# MAGIC 					{ "id": "1002", "type": "Chocolate" },
# MAGIC 					{ "id": "1003", "type": "Blueberry" },
# MAGIC 					{ "id": "1004", "type": "Devil's Food" }
# MAGIC 				]
# MAGIC 		},
# MAGIC 	"topping":
# MAGIC 		[
# MAGIC 			{ "id": "5001", "type": "None" },
# MAGIC 			{ "id": "5002", "type": "Glazed" },
# MAGIC 			{ "id": "5005", "type": "Sugar" },
# MAGIC 			{ "id": "5007", "type": "Powdered Sugar" },
# MAGIC 			{ "id": "5006", "type": "Chocolate with Sprinkles" },
# MAGIC 			{ "id": "5003", "type": "Chocolate" },
# MAGIC 			{ "id": "5004", "type": "Maple" }
# MAGIC 		]
# MAGIC }

# COMMAND ----------


