# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

initSchema = StructType([
    StructField("branch_id",IntegerType(),True),
    StructField("branch_country",StringType(),True),
    StructField("branch_city",StringType(),True)
])

df = spark.read.parquet("/mnt/landing/BranchData/*.parquet", schema=initSchema)
df_flag = df.withColumn("merge_flag",lit(False))
display(df_flag)


# COMMAND ----------

# Make Delta Table Branch
df_flag.write \
    .mode("append") \
    .option("path","/mnt/bronzelayer/BranchData") \
    .saveAsTable("bronzelayer.branch")

# COMMAND ----------

# Move Files to processed container
from datetime import datetime

def filepath(folder_path):
    datenow = datetime.now().strftime("%m-%d-%Y")
    new_folder_path = folder_path + datenow + "/"
    return new_folder_path


# COMMAND ----------

source_path = '/mnt/landing/BranchData/'
destination_path = filepath('/mnt/processed/BranchData/')

dbutils.fs.mv(source_path,destination_path,True)

# COMMAND ----------

display(spark.sql("describe extended bronzelayer.branch"))