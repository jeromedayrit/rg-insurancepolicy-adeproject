# Databricks notebook source
# MAGIC %sql
# MAGIC describe bronzelayer.customer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation

# COMMAND ----------

df = spark.sql("""
               select 
                    customer_id
                    ,first_name
                    ,last_name
                    ,email
                    ,phone
                    ,country
                    ,city
                    ,registration_date
                    ,date_of_birth
                    ,gender
                    ,merge_flag
               from bronzelayer.customer
               where 1=1
               and merge_flag  = false
               and customer_id is not null
               and upper(gender) in ('MALE','FEMALE')
               and registration_date > date_of_birth
               """)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge to SilverLayer Customer

# COMMAND ----------

df.createOrReplaceTempView("clean_customer")

spark.sql(""" 
          MERGE INTO silverlayer.customer sc
          USING clean_customer cc
          ON sc.customer_id = cc.customer_id
          WHEN MATCHED THEN
          UPDATE SET
            sc.customer_id = cc.customer_id
            ,sc.first_name = cc.first_name
            ,sc.last_name = cc.last_name
            ,sc.email = cc.email
            ,sc.phone = cc.phone
            ,sc.country = cc.country
            ,sc.city = cc.city
            ,sc.registration_date = cc.registration_date
            ,sc.date_of_birth = cc.date_of_birth
            ,sc.gender = cc.gender
            ,sc.merge_flag = cc.merge_flag
            ,sc.merge_timestamp = current_timestamp()
         WHEN NOT MATCHED THEN
            INSERT (sc.customer_id,sc.first_name,sc.last_name,sc.email,sc.phone,sc.country,sc.city,sc.registration_date,sc.date_of_birth,sc.gender,sc.merge_flag,sc.merge_timestamp)
            VALUES (cc.customer_id,cc.first_name,cc.last_name,cc.email,cc.phone,cc.country,cc.city,cc.registration_date,cc.date_of_birth,cc.gender,cc.merge_flag,current_timestamp())
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.customer

# COMMAND ----------

# MAGIC %md
# MAGIC ## update bronze.customer merge flag

# COMMAND ----------

spark.sql("update bronzelayer.customer set merge_flag=true where merge_flag=false")