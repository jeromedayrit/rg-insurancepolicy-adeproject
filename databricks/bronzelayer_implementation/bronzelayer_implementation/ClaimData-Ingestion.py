# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

initSchema = StructType([
    StructField("claim_id",IntegerType(),True),
    StructField("policy_id",IntegerType(),True),
    StructField("date_of_claim",IntegerType(),True),
    StructField("claim_amount",IntegerType(),True),
    StructField("claim_status",StringType(),True),
    StructField("LastUpdateTimeStamp",DateType(),True)
])

df = spark.read.parquet("/mnt/landing/ClaimData/*.parquet", schema=initSchema)
df_flag = df.withColumn("merge_flag",lit(False))
display(df_flag)

# COMMAND ----------

#Make Delta table Claim
df_flag.write \
    .mode("append") \
    .option("path","/mnt/bronzelayer/ClaimData") \
    .saveAsTable("bronzelayer.claim")

# COMMAND ----------

#Move File to Process Container
from datetime import datetime

def newfilepath(file_path):
    now = datetime.now().strftime("%m-%d-%Y")
    new_path = file_path + now + '/'
    return new_path


# COMMAND ----------

source_path = "/mnt/landing/ClaimData/"
target_path = newfilepath('/mnt/processed/ClaimData/')
dbutils.fs.mv(source_path,target_path, True)

# COMMAND ----------

display(spark.sql("describe extended bronzelayer.claim"))