# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronzelayer.policy limit 1

# COMMAND ----------

df = spark.sql("""
               select 
                    p.policy_id,
                    p.policy_type,
                    p.customer_id,
                    p.start_date,
                    p.end_date,
                    p.premium,
                    p.coverage_amount,
                    p.merge_flag
               from bronzelayer.policy p
                inner join bronzelayer.customer c
                  on p.customer_id = c.customer_id
               where 1=1
               and p.merge_flag = false
               and p.customer_id is not null
               and p.policy_id is not null
               and p.premium > 0
               and p.coverage_amount > 0
               and p.end_date > p.start_date
               """)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("clean_policy")

spark.sql("""
          MERGE INTO silverlayer.policy sp
          USING clean_policy cp
            ON sp.policy_id = cp.policy_id
          WHEN MATCHED THEN
          UPDATE SET
            sp.policy_type = cp.policy_type
            ,sp.customer_id = cp.customer_id
            ,sp.start_date = cp.start_date
            ,sp.end_date = cp.end_date
            ,sp.premium = cp.premium
            ,sp.coverage_amount = cp.coverage_amount
            ,sp.merge_flag = cp.merge_flag
            ,sp.merge_timestamp = current_timestamp()
         WHEN NOT MATCHED THEN
            INSERT(sp.policy_id,sp.policy_type,sp.customer_id,sp.start_date,sp.end_date,sp.premium,sp.coverage_amount,sp.merge_flag,sp.merge_timestamp)
            VALUES(cp.policy_id,cp.policy_type,cp.customer_id,cp.start_date,cp.end_date,cp.premium,cp.coverage_amount,cp.merge_flag,current_timestamp())

          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC --truncate table silverlayer.policy
# MAGIC select * from silverlayer.policy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update bronze merge flag

# COMMAND ----------

spark.sql("update bronzelayer.policy set merge_flag=true where merge_flag=false")