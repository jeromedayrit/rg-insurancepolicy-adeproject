# Databricks notebook source
# MAGIC %sql
# MAGIC create table goldlayer.claims_analysis
# MAGIC (
# MAGIC   policy_type STRING,
# MAGIC   claim_status STRING,
# MAGIC   avg_claim_amount INT,
# MAGIC   max_claim_amount INT,
# MAGIC   min_claim_amount INT,
# MAGIC   total_claims INT,
# MAGIC   updated_timestamp timestamp 
# MAGIC ) using delta location '/mnt/goldlayer/claims_analysis'