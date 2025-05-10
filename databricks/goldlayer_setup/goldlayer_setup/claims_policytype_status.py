# Databricks notebook source
# MAGIC %sql
# MAGIC create table goldlayer.claims_policytype_status
# MAGIC (
# MAGIC   policy_type STRING,
# MAGIC   claim_status STRING,
# MAGIC   total_claims INT,
# MAGIC   total_claim_amount INT,
# MAGIC   update_timestamp timestamp
# MAGIC )using delta location '/mnt/goldlayer/claims_policytype_status'