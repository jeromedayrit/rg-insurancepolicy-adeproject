# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronzelayer.branch limit 1

# COMMAND ----------

df = spark.sql("""
               select
                    branch_id, 
                    upper(trim(branch_country)) as branch_country, 
                    branch_city, 
                    merge_flag
               from bronzelayer.branch
                where 1=1
                and merge_flag = false
                and branch_id is not null
            """)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge [bronze branch] to [silver branch]

# COMMAND ----------

df.createOrReplaceTempView("clean_branch")

spark.sql("""
        MERGE INTO silverlayer.branch b
        USING clean_branch as c
          ON b.branch_id = c.branch_id
      
        WHEN MATCHED THEN 
        UPDATE
          SET b.branch_country = c.branch_country,
              b.branch_city = c.branch_city,
              b.merge_flag = true,
              b.merge_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
        INSERT (b.branch_id, b.branch_country,b.branch_city,b.merge_flag,b.merge_timestamp)
          VALUES(c.branch_id,C.branch_country,c.branch_city,c.merge_flag,current_timestamp())
        """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.branch

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update merge flag

# COMMAND ----------

spark.sql("update silverlayer.branch set merge_flag = true where merge_flag = false")