# Databricks notebook source
from datetime import datetime, timedelta
import dateutil.relativedelta
import traceback
import logging
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.functions import row_number

# COMMAND ----------

today=datetime(2022,6,21)
offset = ((today.weekday() - 6) % 7)#monday->1 sunday->7 
twoYearEnd = ((today - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")#41 date of last saturday?
twoYearStart_tmp=((today+ dateutil.relativedelta.relativedelta(weeks=-104)))#41 2 years before today
offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)# weekday of "2 years before today"
twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")# the last sunday before "2 years before today"
OpenYrMth = today.strftime("%y"+"%m"+"%d")#today
print("OpenYrMth  is ", OpenYrMth)
print("twoYearStart  is ", twoYearStart)
print("twoYearEnd  is ", twoYearEnd)
print("Start Rogers Customer base")

# COMMAND ----------

# #min date- 2020-07-21
# #max date- 2022-05-18
# import datetime
# today = datetime.datetime(2022, 5, 27)
# offset = ((today.weekday() - 6) % 7)
# twoYearEnd = ((today - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")
# twoYearStart_tmp=((today+ dateutil.relativedelta.relativedelta(weeks=-104)))
# offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)
# twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")
# OpenYrMth = today.strftime("%y"+"%m"+"%d")
# print("OpenYrMth  is ", OpenYrMth)
# print("twoYearStart  is ", twoYearStart)
# print("twoYearEnd  is ", twoYearEnd)
# print("Start Rogers Customer base")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rogers

# COMMAND ----------

 ##Legacy and ignite base query 1
 ##TODO: should activity date be between two dates?
#CAN: company+account_id  or account_id
#41 input1:APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST
#41 ATTR_ADDRESS_SERVICEBILITY = 'P'
#twoYearEnd (activity date): last saturday
#41 question why two queries?


df1_leg_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT as account_id,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Rogers' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd)).cache()
print(df1_leg_base_1.count())
print(df1_leg_base_1.select('ECID').distinct().count())



# COMMAND ----------

#df1_leg_base_2 depends on df1_leg_base_1, and df1_leg_base_1 take 'today' as paramater 
#CAN: company+account_id  or account_id
#tenure_month=0 means the activity date is within one month before today
#41 the tenure_month would always be 0

df1_leg_base_2=df1_leg_base_1.select(col("CAN").alias("ban_can"),col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer')))

# COMMAND ----------

print("Deduplicate") 
#only count once for each ecid
df1_merged_base_2=df1_leg_base_2.where(col("ecid").isNotNull()).groupby("ecid").agg(F.max("tenure_month").alias('tenure_month')).cache()
#1957015
print(df1_merged_base_2.count())
#TODO: replace schema and model_no_r with sys.args
# schema='mlmodel_data'
# model_no_r='test_sg'
# display(df1_merged_base_2)
# print("writing merged Rogers base customer query into schema " + schema)
# df1_merged_base_2.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no_r+"_r_customer_base")
# print(schema+".DAT1_"+model_no_r+"_r_customer_base generated successfully")



# COMMAND ----------

#only pick the overlapping ecid
overlap_ecid=spark.sql("select ecid from hive_metastore.mlmodel_data.dat1_123_yuncheng_rogers_overlap")
overlap_ecid.createOrReplaceTempView("overlap_ecid")
df1_merged_base_2.createOrReplaceTempView("df1_merged_base_2")
df1_merged_base_2_overlap=spark.sql("select df1_merged_base_2.* from df1_merged_base_2 join overlap_ecid on df1_merged_base_2.ecid==overlap_ecid.ecid")

# COMMAND ----------

df1_merged_base_2_overlap.count()

# COMMAND ----------

df1_merged_base_2_overlap.show()

# COMMAND ----------

schema='mlmodel_data'
model_no_r='123_yuncheng' 
print("writing merged Rogers base customer query into schema " + schema)
df1_merged_base_2_overlap.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no_r+"_r_customer_base")
print(schema+".DAT1_"+model_no_r+"_r_customer_base generated successfully")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Fido

# COMMAND ----------

#landed --app_maestro.Vw_PntrDly-- APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST into azure

df1_query_fido_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Fido' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd))

# APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST
# ATTR_ADDRESS_SERVICEBILITY = 'P'
# tenure_month supposed to be all zero

df1_query_fido_base_2=df1_query_fido_base_1.select(col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE")).withColumn("tenure_month",abs(months_between(to_date("ACTIVITY_DATE"), current_date()).cast('integer'))).where(col("ECID").isNotNull()).groupby("ECID").agg(F.max("tenure_month").alias('tenure_month'))



# COMMAND ----------

df1_query_fido_base_2.count()

# COMMAND ----------


schema='mlmodel_data'
model_no='124_yuncheng' 
print("writing merged Fido base customer query into schema " + schema +".DAT1_"+model_no+"_f_customer_base")
df1_query_fido_base_2.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no+"_f_customer_base")
print(schema+".DAT1_"+model_no+"_f_customer_base generated successfully")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mlmodel_data.DAT1_124_f_customer_base

# COMMAND ----------


