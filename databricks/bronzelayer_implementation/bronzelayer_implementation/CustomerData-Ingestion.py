# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *


initSchema = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("first_name",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("email",StringType(),True),
    StructField("phone",StringType(),True),
    StructField("country",StringType(),True),
    StructField("city",StringType(),True),
    StructField("registration_date",DateType(),True),
    StructField("date_of_birth",DateType(),True),
    StructField("gender",StringType(),True)
])


df = spark.read.csv("/mnt/landing/Customer/",header=True,schema=initSchema)
df_flag = df.withColumn("merge_flag",lit(False))
display(df_flag)


# COMMAND ----------

#Make Delta Table customer

df_flag.write \
    .mode("append") \
    .option("path","/mnt/bronzelayer/Customer") \
    .saveAsTable("bronzelayer.customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from bronzelayer.customer
# MAGIC limit 1

# COMMAND ----------

from datetime import datetime

def get_current_datetime(folder_path):
    now = datetime.now().strftime("%m-%d-%Y")
    new_path = folder_path + now
    return new_path

# COMMAND ----------

source_path = "/mnt/landing/Customer/"
sink_path = get_current_datetime("/mnt/processed/Customer/")

dbutils.fs.mv(source_path,sink_path,True)