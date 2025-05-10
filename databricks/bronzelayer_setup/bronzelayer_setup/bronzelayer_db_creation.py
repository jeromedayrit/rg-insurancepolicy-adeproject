# Databricks notebook source
# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database bronzelayer;

# COMMAND ----------

#Using Spart Command
#spark.sql("drop database bronzelayer")
spark.sql("create database bronzelayer")

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronzelayer;
# MAGIC show tables;