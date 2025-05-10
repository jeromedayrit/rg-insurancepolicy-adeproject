# Databricks notebook source
display(spark.sql("describe extended bronzelayer.policy"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silverlayer.policy
# MAGIC (
# MAGIC   policy_id INT
# MAGIC   ,policy_type STRING
# MAGIC   ,customer_id INT
# MAGIC   ,start_date DATE
# MAGIC   ,end_date DATE
# MAGIC   ,premium INT
# MAGIC   ,coverage_amount INT
# MAGIC   ,merge_flag boolean
# MAGIC   ,merge_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Policy'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended silverlayer.policy