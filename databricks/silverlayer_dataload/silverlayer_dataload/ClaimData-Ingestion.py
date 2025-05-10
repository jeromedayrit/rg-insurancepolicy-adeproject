# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronzelayer.claim limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform

# COMMAND ----------

df= spark.sql("""
              select c.claim_id
                    ,p.policy_id
                    ,to_date(c.date_of_claim,'MM-dd-yyyy') as date_of_claim--,to_date(date_format(c.date_of_claim,'mm-dd-yyyy'),'MM-dd-yyyy') as date_of_claim
                    ,c.claim_amount
                    ,c.claim_status
                    ,c.LastUpdatedTimeStamp
                    ,c.merge_flag
              from bronzelayer.claim c
              inner  join bronzelayer.policy p
                    on c.policy_id = p.policy_id
              where 1=1
              and c.merge_flag = false
              and c.claim_amount > 0
              """)
df = df.na.drop()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge to silverlayer.claim

# COMMAND ----------

df.createOrReplaceTempView("clean_claim")
spark.sql("""
          MERGE INTO silverlayer.claim sc
          USING clean_claim cc
            ON  sc.policy_id = cc.policy_id
         WHEN MATCHED THEN
         UPDATE SET
            sc.claim_id = cc.claim_id
            ,sc.policy_id = cc.policy_id
            ,sc.date_of_claim = cc.date_of_claim
            ,sc.claim_amount = cc.claim_amount
            ,sc.claim_status = cc.claim_status
            ,sc.LastUpdatedTimeStamp = cc.LastUpdatedTimeStamp
            ,sc.merge_flag = cc.merge_flag
            ,sc.merge_timestamp = current_timestamp()

         WHEN NOT MATCHED THEN
            INSERT(sc.claim_id,sc.policy_id,sc.date_of_claim,sc.claim_amount,sc.claim_status,sc.LastUpdatedTimeStamp,sc.merge_flag,sc.merge_timestamp )
            VALUES(cc.claim_id,cc.policy_id,cc.date_of_claim,cc.claim_amount,cc.claim_status,cc.LastUpdatedTimeStamp,cc.merge_flag,current_timestamp())
    
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.claim limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update merge_flag = True

# COMMAND ----------

spark.sql("update bronzelayer.claim set merge_flag=true where merge_flag=false")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.claim