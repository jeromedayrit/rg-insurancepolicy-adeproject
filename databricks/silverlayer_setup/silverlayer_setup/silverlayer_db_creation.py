# Databricks notebook source
# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

spark.sql("create database silverlayer")

# COMMAND ----------

# MAGIC %sql
# MAGIC use silverlayer;
# MAGIC show tables