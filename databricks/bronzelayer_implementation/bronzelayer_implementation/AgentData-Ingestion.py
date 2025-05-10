# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

initSchema = StructType([
    StructField("agent_id",IntegerType(),True),
    StructField("agent_name",StringType(),True),
    StructField("agent_email",StringType(),True),
    StructField("agent_phone",StringType(),True),
    StructField("branch_id",IntegerType(),True),
    StructField("create_timestamp",DateType(),True)
])

df = spark.read.parquet("/mnt/landing/AgentData/*.parquet", schema=initSchema)
df_flag = df.withColumn("merge_flag",lit(False))
display(df_flag)


# COMMAND ----------

# Create Delta Table for Agent
df_flag.write \
    .mode("append") \
    .option("path","/mnt/bronzelayer/AgentData") \
    .saveAsTable("bronzelayer.agent")

# COMMAND ----------

# MAGIC %sql
# MAGIC --use bronzelayer;
# MAGIC --show tables;
# MAGIC select * from bronzelayer.agent

# COMMAND ----------





# COMMAND ----------

from datetime import datetime

current_time = datetime.now().strftime("%m-%d-%Y")
new_folder_path = f'/mnt/processed/AgentData/{current_time}/'
dbutils.fs.mv('/mnt/landing/AgentData/',new_folder_path,True)


# COMMAND ----------

display(spark.sql("describe extended bronzelayer.agent"))