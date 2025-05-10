# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table silverlayer.agent(
# MAGIC   agent_id integer,
# MAGIC   agent_name string,
# MAGIC   agent_email string,
# MAGIC   agent_phone string,
# MAGIC   branch_id integer,
# MAGIC   create_timestamp timestamp,
# MAGIC   merge_timestamp timestamp
# MAGIC ) using delta location '/mnt/silverlayer/Agent'

# COMMAND ----------

# MAGIC %sql
# MAGIC use silverlayer;
# MAGIC show tables