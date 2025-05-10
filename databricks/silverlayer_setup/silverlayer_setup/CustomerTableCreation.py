# Databricks notebook source
# MAGIC %sql
# MAGIC create table silverlayer.customer
# MAGIC (
# MAGIC   customer_id	int,
# MAGIC   first_name	string,
# MAGIC   last_name	string,
# MAGIC   email	string,
# MAGIC   phone	string,
# MAGIC   country	string,
# MAGIC   city	string,
# MAGIC   registration_date	date,
# MAGIC   date_of_birth	date,
# MAGIC   gender	string,
# MAGIC   merge_flag	boolean,
# MAGIC   merge_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Customer'

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe silverlayer.customer