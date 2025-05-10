# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


initSchema = StructType([
    StructField("policy_id", IntegerType(),True),
    StructField("policy_type",StringType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("start_date",DateType(),True), 
    StructField("end_date",DateType(),True),
    StructField("premium",IntegerType(),True),
    StructField("coverage_amount",IntegerType(),True)
])


#df = spark.read.json("/mnt/landing/PolicyData", schema=initSchema)
#df = spark.read.option("multiline","true").json("/mnt/landing/PolicyData", schema=initSchema)
#fetch 1st row
#df = spark.read.format("json").option("multiline","true").load("/mnt/landing/PolicyData", schema=initSchema)
df_flag = df.withColumn("merge_flag", lit(False))

display(df_flag)


# COMMAND ----------

# Make Delta Table PolicyData
df_flag.write \
    .mode("append") \
    .option("path","/mnt/bronzelayer/PolicyData") \
    .saveAsTable("bronzelayer.policy")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.policy

# COMMAND ----------

from datetime import datetime

def get_current_datetime(folder_path):
    now = datetime.now().strftime("%m-%d-%Y")
    new_path = folder_path + now
    return new_path

# COMMAND ----------

source_path = "/mnt/landing/PolicyData"
target_path = get_current_datetime("/mnt/processed/PolicyData/")

dbutils.fs.mv(source_path,target_path,True)