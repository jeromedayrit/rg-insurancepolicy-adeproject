# Databricks notebook source
# MAGIC %sql
# MAGIC use silverlayer ;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silverlayer.branch (
# MAGIC 	branch_id INT,
# MAGIC 	branch_country STRING,
# MAGIC 	branch_city STRING,
# MAGIC 	merge_flag STRING,
# MAGIC 	merge_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Branch'
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended silverlayer.branch