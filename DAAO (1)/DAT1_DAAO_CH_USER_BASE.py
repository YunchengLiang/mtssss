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

offset = ((datetime.today().weekday() - 6) % 7) #monday->1 sunday->7 
twoYearEnd = ((datetime.today() - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")#41 date of last saturday?
twoYearStart_tmp=((datetime.today()+ dateutil.relativedelta.relativedelta(weeks=-104)))#41 2 years before today
offset2 = ((twoYearStart_tmp.weekday() - 6) % 7) # weekday of "2 years before today"
twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d") # the last saturday before "2 years before today"
OpenYrMth = datetime.today().strftime("%y"+"%m"+"%d") #today
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

 ##Legacy base query 1
 ##TODO: No Results/marquee.address table (d) is empty
#CAN: company+account_id  or account_id
#41 input1:APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST
#41 ATTR_ADDRESS_SERVICEBILITY = 'P'
#twoYearEnd (activity date): last saturday
#41 (PRODUCT_SOURCE like '%SS%' or PRODUCT_SOURCE like '%MS%') another constraint？ 
#41 question why two queries?

query_leg_base="""select CUSTOMER_ID,CUSTOMER_ACCOUNT as account_id,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Rogers' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'"""

print("Legacy base query 1 ", query_leg_base)
df1_leg_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT as account_id,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Rogers' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and (PRODUCT_SOURCE like '%SS%' or PRODUCT_SOURCE like '%MS%') and ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd)).cache()
print(df1_leg_base_1.count())
##38886 rows

# COMMAND ----------

df1_leg_base_1.select('ECID').distinct().count()

# COMMAND ----------

display(df1_leg_base_1)

# COMMAND ----------

df1_leg_base_1.select("PRODUCT_SOURCE").distinct().show()

# COMMAND ----------

#df1_leg_base_2 depends on df1_leg_base_1, and df1_leg_base_1 take 'today' as paramater 
#CAN: company+account_id  or account_id
#tenure_month=0 means the activity date is within one month before today
#41 so is query2 used to investigate past date data? if so it should not depend on query1, otherwise the tenure_month would always be 0
#41 why not cache
df1_leg_base_2=df1_leg_base_1.select(col("CAN").alias("ban_can"),col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer'))).withColumn("type",lit("Legacy"))

# COMMAND ----------

df1_leg_base_2.show(100)

# COMMAND ----------

##Ignite base query 1
##TODO: what data does ignite contain? Rogers Ignite
#41 app_maestro.acctdim, app_ibro.ibro_household_closing, hash_lookup.ecid_ban_can
# app_ibro.ibro_household_closing(B) used as contraint to filter account_id for app_maestro.acctdim(A)
# hash_lookup.ecid_ban_can(C) used to add info(hash_count) to app_maestro.acctdim(A)
# where A.maestro_ind='Y' and A.crnt_f='Y' --- more contraints for app_maestro.acctdim(A)
query_ignite_base="""select ACCOUNT_ID,ACCOUNT_STATUS,AR_ACCOUNT_SUB_TYPE,AR_ACCOUNT_TYPE,X_LOB,x_acquision_date,x_start_date,WIRELESS_ACCOUNT,EC_ID,X_CNSLD_ACCNT_ID,BRAND,c.hash_account from app_maestro.acctdim A INNER JOIN (select DISTINCT customer_account from app_ibro.ibro_household_closing  where report_date between to_date('"""+twoYearStart+"""') and to_date('"""+twoYearEnd+"""') and multi_system like '%MS%' AND MULTI_BRAND LIKE '%ROGERS%' and multi_product_name like '%TV%INT%') B ON A.ACCOUNT_ID=B.customer_account left join hash_lookup.ecid_ban_can c on a.account_id=c.account where A.maestro_ind='Y' and A.crnt_f='Y'"""

print("Ignite base query 1 ", query_ignite_base)
df1_query_ignite_base_1=spark.sql(query_ignite_base).cache()

# account_id from A, hash_account from C(may have null value cause leftjoin), x_acquision_date from A
#41 what does tenure_dt(activity date) has to do with legacy base activity date(last saturday)
#41 not like in legacy table, it is possible that the tenure_dt is one month or more before current_date, in other words, tenure month could exceed 0
df1_query_ignite_base_2=df1_query_ignite_base_1.select(col("account_id").alias("ban_can"),col("hash_account").alias("hash_ban_can"),col("EC_ID").alias("ecid"),col("x_acquision_date").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer'))).withColumn("type",lit("Ignite"))



# COMMAND ----------

df1_query_ignite_base_2.select(count(when(isnan('hash_ban_can'),1)).alias('hash_ban_can_nulls')).show()

# COMMAND ----------

df1_query_ignite_base_2.describe().show()#41 tenure month 3233?

# COMMAND ----------

df1_query_ignite_base_2.show(50)

# COMMAND ----------

df1_leg_base_2.show(50)

# COMMAND ----------

print(df1_query_ignite_base_2.count())

# COMMAND ----------

df1_leg_base_2.createOrReplaceTempView('dlegbase_2')
result=spark.sql('select distinct(left(hash_ban_can,2)) as prefix from dlegbase_2')
result.show()

# COMMAND ----------

df1_query_ignite_base_2.createOrReplaceTempView('dfigbase_2')
result2=spark.sql(" select * from dfigbase_2 where left(hash_ban_can,2)=='K1' ")
result2.show()
#good result shows no overlap between two sources 

# COMMAND ----------

#Merging the two tables
print("Merge legacy and Ignite")
df1_merged_base_1 = df1_leg_base_2.union(df1_query_ignite_base_2)        

# COMMAND ----------

df1_merged_base_1.groupby("ecid").count().show()

# COMMAND ----------

#Merging the two tables
print("Merge legacy and Ignite")
df1_merged_base_1 = df1_leg_base_2.union(df1_query_ignite_base_2)        

print("Deduplicate") 
#asc("type") would rank 'ignite' first
# max("tenure_month") would prefer 'ignite 'first since all 'legacy' tenure_month are 0
# max(when(col("row_num") == 1,col("type")).otherwise(None)) would choose the type for row_num=1, in other words, preferably 'ignite'
# ouput is tenure_month and type
# in otherwords for each ecid, if we have data from both "ignite" and "legacy", we only keep "ignite" , and only one for each ecid
df1_merged_base_2=df1_merged_base_1.where(col("ecid").isNotNull()).withColumn("row_num", row_number().over(Window.partitionBy("ecid").orderBy(asc("type")))).groupby("ecid").agg(F.max("tenure_month").alias('tenure_month'),F.max(when(col("row_num") == 1,col("type")).otherwise(None)).alias('type')).drop("row_num").cache()

print(df1_merged_base_2.count())
#TODO: replace schema and model_no_r with sys.args
# schema='mlmodel_data'
# model_no_r='test_sg'
# display(df1_merged_base_2)
# print("writing merged Rogers base customer query into schema " + schema)
# df1_merged_base_2.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no_r+"_r_customer_base")
# print(schema+".DAT1_"+model_no_r+"_r_customer_base generated successfully")



# COMMAND ----------

df1_merged_base_2.groupby('type').count().show()

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

# APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST
# ATTR_ADDRESS_SERVICEBILITY = 'P'
# tenure_month supposed to be all zero
# not like rogers, don't have type
df1_query_fido_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Fido' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd))

df1_query_fido_base_2=df1_query_fido_base_1.select(col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE")).withColumn("tenure_month",abs(months_between(to_date("ACTIVITY_DATE"), current_date()).cast('integer'))).where(col("ECID").isNotNull()).groupby("ECID").agg(F.max("tenure_month").alias('tenure_month'))




# COMMAND ----------

df1_query_fido_base_2.show(20)

# COMMAND ----------

df1_query_fido_base_2.select('tenure_month').distinct().show()

# COMMAND ----------

schema='mlmodel_data'
model_no='124'
print("writing merged Fido base customer query into schema " + schema +".DAT1_"+model_no+"_f_customer_base")
df1_query_fido_base_2.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no+"_f_customer_base")
print(schema+".DAT1_"+model_no+"_f_customer_base generated successfully")
