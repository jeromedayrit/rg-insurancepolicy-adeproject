# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE goldlayer.sales_policy_month
# MAGIC (
# MAGIC   policy_type STRING,
# MAGIC   sales_month STRING,
# MAGIC   total_premium INT,
# MAGIC   update_timestamp TIMESTAMP
# MAGIC )using DELTA location '/mnt/goldlayer/sales_policy_month'