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

offset = ((datetime.today().weekday() - 6) % 7)#monday->1 sunday->7 
twoYearEnd = ((datetime.today() - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")#41 date of last saturday?
twoYearStart_tmp=((datetime.today()+ dateutil.relativedelta.relativedelta(weeks=-104)))#41 2 years before today
offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)# weekday of "2 years before today"
twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")# the last sunday before "2 years before today"
OpenYrMth = datetime.today().strftime("%y"+"%m"+"%d")#today
print("OpenYrMth  is ", OpenYrMth)
print("twoYearStart  is ", twoYearStart)
print("twoYearEnd  is ", twoYearEnd)
print("Start Rogers Customer base")

 ##Ignite base query 1
        query_ignite_base="""select ACCOUNT_ID,ACCOUNT_STATUS,AR_ACCOUNT_SUB_TYPE,AR_ACCOUNT_TYPE,X_LOB,x_acquision_date,x_start_date,WIRELESS_ACCOUNT,EC_ID,X_CNSLD_ACCNT_ID,BRAND,c.hash_account from app_maestro.acctdim A INNER JOIN (select DISTINCT customer_account from app_ibro.ibro_household_closing  where report_date between to_date('2020-06-21') and to_date('2022-06-18') and multi_system like '%MS%' AND MULTI_BRAND LIKE '%ROGERS%' and multi_product_name like '%TV%INT%') B ON A.ACCOUNT_ID=B.customer_account left join hash_lookup.ecid_ban_can c on a.account_id=c.account where A.maestro_ind='Y' and A.crnt_f='Y'"""
        


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

query_leg_base="""select CUSTOMER_ID,CUSTOMER_ACCOUNT as account_id,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Rogers' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'"""

print("Legacy and Ignite base query 1 ", query_leg_base)
df1_leg_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT as account_id,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Rogers' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd)).cache()
print(df1_leg_base_1.count())
##38886 rows



# COMMAND ----------

df1_leg_base_1.select('ECID').distinct().count()#3890436->1938423->1957015

# COMMAND ----------

display(df1_leg_base_1)

# COMMAND ----------

df1_leg_base_1.select("PRODUCT_SOURCE").distinct().show()#MS means ignite, SS means legacy

# COMMAND ----------

#df1_leg_base_2 depends on df1_leg_base_1, and df1_leg_base_1 take 'today' as paramater 
#CAN: company+account_id  or account_id
#tenure_month=0 means the activity date is within one month before today
#41 the tenure_month would always be 0

df1_leg_base_2=df1_leg_base_1.select(col("CAN").alias("ban_can"),col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer')))

# COMMAND ----------

display(df1_leg_base_2)

# COMMAND ----------

df1_leg_base_2.select('tenure_dt').distinct().show()

# COMMAND ----------

print("Deduplicate") 
#only count once for each ecid
df1_merged_base_2=df1_leg_base_2.where(col("ecid").isNotNull()).groupby("ecid").agg(F.max("tenure_month").alias('tenure_month')).cache()
#1938450
print(df1_merged_base_2.count())
#TODO: replace schema and model_no_r with sys.args
# schema='mlmodel_data'
# model_no_r='test_sg'
# display(df1_merged_base_2)
# print("writing merged Rogers base customer query into schema " + schema)
# df1_merged_base_2.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no_r+"_r_customer_base")
# print(schema+".DAT1_"+model_no_r+"_r_customer_base generated successfully")



# COMMAND ----------

schema='mlmodel_data'
model_no_r='123_yuncheng' 
print("writing merged Rogers base customer query into schema " + schema)
df1_merged_base_2.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no_r+"_r_customer_base")
print(schema+".DAT1_"+model_no_r+"_r_customer_base generated successfully")


# COMMAND ----------

display(df1_merged_base_2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fido

# COMMAND ----------

#landed --app_maestro.Vw_PntrDly-- APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST into azure
'''
old query-
query_fido_base="""select a.account_id  ,b.account_status ,b.ar_account_sub_type ,b.ar_account_type ,b.x_conv_ind ,b.x_hh_id ,b.x_lob ,b.wireless_account ,b.ec_id ,b.x_acquision_date ,b.contact_key ,b.x_is_cnsld ,b.x_cnsld_accnt_id ,b.lob_ind ,b.account_status_code ,b.brand ,c.hash_account from ( select CUSTOMER_ACCOUNT as account_id FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Fido' and CUSTOMER_SEGMENT = 'CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and ACTIVITY_DATE ='{twoYearEnd}') a inner join app_maestro.acctdim b on a.account_id = b.account_id and b.crnt_f='Y' and b.maestro_ind='Y' left join hash_lookup.ecid_ban_can c on a.account_id = c.account".format(twoYearEnd)"""
print("Fido base query 1 ", query_fido_base)
'''
df1_query_fido_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Fido' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd))

# APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST
# ATTR_ADDRESS_SERVICEBILITY = 'P'
# tenure_month supposed to be all zero

df1_query_fido_base_2=df1_query_fido_base_1.select(col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE")).withColumn("tenure_month",abs(months_between(to_date("ACTIVITY_DATE"), current_date()).cast('integer'))).where(col("ECID").isNotNull()).groupby("ECID").agg(F.max("tenure_month").alias('tenure_month'))



# COMMAND ----------

df1_query_fido_base_2.count()

# COMMAND ----------

display(df1_query_fido_base_2)

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


