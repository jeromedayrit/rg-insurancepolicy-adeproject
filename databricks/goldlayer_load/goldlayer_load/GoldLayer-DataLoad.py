# Databricks notebook source
# MAGIC %md
# MAGIC <b> Sales By Policy Type and Month: </b>
# MAGIC This table would contain the total sales for each policy type and each month. It would be used to analyze the performance of different policy types over time.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


_dfsales = spark.sql("""select policy_type, 
                    start_date as sales_month, 
                    sum(premium) as total_premium, 
                    current_timestamp() as update_timestamp
                    from silverlayer.policy
                    group by policy_type, start_date
                    having policy_type is not null
                    order by policy_type, start_date """)
display(_dfsales)

# COMMAND ----------

_dfsales.createOrReplaceTempView("clean_sales")
spark.sql("""
          merge into goldlayer.sales_policy_month sp
          using clean_sales  cs
            on sp.policy_type = cs.policy_type
         when matched then
         update set
            sp.sales_month = cs.sales_month,
            sp.total_premium = cs.total_premium,
            sp.update_timestamp = current_timestamp()
          when not matched then
          insert (sp.policy_type,sp.sales_month, sp.total_premium,sp.update_timestamp)
          values(cs.policy_type,cs.sales_month,cs.total_premium, current_timestamp())
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC --truncate table goldlayer.sales_policy_month
# MAGIC select * from goldlayer.sales_policy_month limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### <b>Claims By Policy Type and Status:</b>
# MAGIC  This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.

# COMMAND ----------

#df.createOrReplaceTempView("policy_data")
_dfclaim = spark.sql("""
            select 
                p.policy_type, 
                c.claim_status, 
                count(c.claim_id) as total_claims, 
                sum(c.claim_amount) as total_claim_amount, 
                current_timestamp() as update_timestamp
            from silverlayer.claim c
                inner join silverlayer.policy p
                on c.policy_id = p.policy_id
            group by p.policy_type, c.claim_status
            having p.policy_type is not null
            order by p.policy_type, c.claim_status
               """)
display(_dfclaim)

# COMMAND ----------

#%sql
#select * from goldlayer.claims_policytype_status
_dfclaim.createOrReplaceTempView("clean_claims_pol_stat")

spark.sql("""
          merge into goldlayer.claims_policytype_status cps
          using clean_claims_pol_stat ccp
            on  cps.policy_type = ccp.policy_type
                and cps.claim_status = ccp.claim_status
            when matched then
            update set
                cps.claim_status = ccp.claim_status,
                cps.total_claims = ccp.total_claims,
                cps.total_claim_amount = ccp.total_claim_amount,
                cps.update_timestamp = current_timestamp()
            when not matched then
                insert(cps.policy_type,cps.claim_status,cps.total_claims,cps.total_claim_amount,cps.update_timestamp)
                values(ccp.policy_type,ccp.claim_status,ccp.total_claims,ccp.total_claim_amount,current_timestamp())
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  goldlayer.claims_policytype_status

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim.

# COMMAND ----------

_dfclaimAgg = spark.sql("""
                     select 
                        p.policy_type,
                        c.claim_status,
                        round(avg(c.claim_amount),2) as avg_claim_amount,
                        max(c.claim_amount) as max_claim_amount,
                        min(c.claim_amount) as min_claim_amount,
                        count(c.claim_id) as total_count_claim
                     from silverlayer.claim c
                        inner join silverlayer.policy p
                        on c.policy_id = p.policy_id
                    group by 
                        p.policy_type,
                        c.claim_status
                    having p.policy_type is not null
                    order by p.policy_type, c.claim_status
                     """)
display(_dfclaimAgg)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.claims_analysis

# COMMAND ----------



_dfclaimAgg.createOrReplaceTempView("clean_claim_analysis")

spark.sql("""
          merge into goldlayer.claims_analysis ca
          using clean_claim_analysis cc
            on ca.policy_type = cc.policy_type
              and ca.claim_status = cc.claim_status
          when matched then
          update set
              ca.avg_claim_amount = cc.avg_claim_amount,
              ca.max_claim_amount = cc.max_claim_amount,
              ca.min_claim_amount = cc.min_claim_amount,
              ca.total_claims = cc.total_count_claim,
              ca.updated_timestamp = current_timestamp()
          when not matched then
          insert(ca.policy_type,ca.claim_status,ca.avg_claim_amount,ca.max_claim_amount,ca.min_claim_amount,ca.total_claims,ca.updated_timestamp)
          values(cc.policy_type,cc.claim_status,cc.avg_claim_amount,cc.max_claim_amount,cc.min_claim_amount,cc.total_count_claim,current_timestamp())
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC --truncate table goldlayer.claims_analysis
# MAGIC select * from goldlayer.claims_analysis