# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.sql("select * from bronzelayer.Agent where merge_flag = False")
display(df)

# COMMAND ----------

dfbranch = spark.sql("select * from bronzelayer.branch where merge_flag = false")
display(dfbranch)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove all rows where Branch Id not exist in Branch Table

# COMMAND ----------

df1 = df.join(dfbranch, df.branch_id == dfbranch.branch_id,"inner")
df1 = df1.select(col("agent_id"),col("agent_name"),col("agent_email"),col("agent_phone"),df.branch_id,col("create_timestamp"))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure all the phone have a valid 10 digit phone no.

# COMMAND ----------

df2 = df1.where(length(col("agent_phone")) == 10)
#df2 = df1.withColumn(rpad(col("agent_phone_10")),10,0)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replace all the null email with 'admin@azurelib.com'

# COMMAND ----------

#df3 = df2.na.fill('admin@azurelib.com', subset=["agent_email"])

df3 = df2.withColumn("agent_email_null", \
                        when(col("agent_email") == '', lit("admin@azurelib.com")) \
                        .when(col("agent_email").isNull(),lit("admin@azurelib.com")) \
                        .otherwise(col("agent_email"))
                        )
df3 = df3.select("agent_id", "agent_name", "agent_email_null", "agent_phone", "branch_id", "create_timestamp")
df3 = df3.withColumnRenamed("agent_email_null","agent_email")
#dfnull = df3.where(col("agent_email") == '')
display(df3)
#display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add the merge_date_timestamp (current timestamp)

# COMMAND ----------

df4 = df3.withColumn("merge_date_timestamp",lit(current_timestamp()))
display(df4)

# COMMAND ----------

df4.createOrReplaceTempView("cleandata")
spark.sql("""
    merge into silverlayer.agent as t 
    using cleandata as s on t.agent_id = s.agent_id
    when matched then
        update set 
            t.agent_name = s.agent_name,
            t.agent_email = s.agent_email,
            t.agent_phone = s.agent_phone
    when not matched then
        insert(t.agent_id, t.agent_name, t.agent_email, t.agent_phone, t.branch_id, t.create_timestamp, t.merge_timestamp)
        values(s.agent_id, s.agent_name, s.agent_email, s.agent_phone, s.branch_id, s.create_timestamp, current_timestamp())    
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.agent

# COMMAND ----------

spark.sql("update bronzelayer.agent set merge_flag =true where merge_flag = false")