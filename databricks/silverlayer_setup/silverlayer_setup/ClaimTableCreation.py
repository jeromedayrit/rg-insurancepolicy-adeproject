# Databricks notebook source
# MAGIC %sql
# MAGIC use silverlayer ;
# MAGIC show tables

# COMMAND ----------

display(spark.sql("describe bronzelayer.claim"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silverlayer.claim
# MAGIC (
# MAGIC     claim_id	int
# MAGIC     ,policy_id	int
# MAGIC     ,date_of_claim	date
# MAGIC     ,claim_amount	decimal(18,0)
# MAGIC     ,claim_status	string
# MAGIC     ,LastUpdatedTimeStamp	timestamp
# MAGIC     ,merge_flag	boolean
# MAGIC     ,merge_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Claim'
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended silverlayer.claim