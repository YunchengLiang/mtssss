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

# MAGIC %sql
# MAGIC refresh table APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST;

# COMMAND ----------

 ##Legacy and ignite base query 1
 ##TODO: should activity date be between two dates?
#CAN: company+account_id  or account_id
#41 input1:APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST
#41 ATTR_ADDRESS_SERVICEBILITY = 'P'
#twoYearEnd (activity date): last saturday

df1_leg_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT as account_id,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Rogers' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd)).cache()
print(df1_leg_base_1.count())
print(df1_leg_base_1.select('ECID').distinct().count())
print("*******************")
df1_leg_base_1_attr_filtered=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT as account_id,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Rogers' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd)).cache()
print(df1_leg_base_1_attr_filtered.count())
print(df1_leg_base_1_attr_filtered.select('ECID').distinct().count())
#distinct ecid increased 1938423->1957015 (about 18,592 increase) after deleting condition ATTR_ADDRESS_SERVICEBILITY = 'P'

# COMMAND ----------

df1_leg_base_1_attr_filtered.createOrReplaceTempView("attr_filtered")
df1_leg_base_1.createOrReplaceTempView("df1_leg_base")

# COMMAND ----------

increase_base = spark.sql("""
            select distinct ECID, CUSTOMER_COMPANY,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE 
            from  attr_filtered
            where attr_filtered.ECID not in (select distinct ECID from df1_leg_base)
         """)
print(increase_base.count())#almost the same with "18592 increase"

# COMMAND ----------

#product source ***
print(increase_base.groupby("PRODUCT_SOURCE").count().show())#MS means ignite, SS means legacy
#major changes for data increase comes from MS(ignite), ABOUT 18529
print("*****************")

#CUSTOMER_MUTI_PRODUCT ***
print(increase_base.groupby("CUSTOMER_COMPANY").count().show())
#major changes for data increase comes from NULL(Company), ABOUT 18529, iginite's company is null right?
print("*****************")

#CUSTOMER_MUTI_PRODUCT *
print(increase_base.groupby("CUSTOMER_MULTI_PRODUCT").count().show())

# COMMAND ----------

# MAGIC %md
# MAGIC ##but still, the total data count is 1,957,015 whcich is far from 2,127,671,    
# MAGIC ##we need explanation for the 170,656 data loss

# COMMAND ----------

# MAGIC %md
# MAGIC ##original query for legacy and ignite     
# MAGIC query_leg_base="""select MQ.*, OD.BAN, hash.hash_account, hash.ecid from(select CONCAT(a.COMPANY_NUMBER,a.ACCOUNT_NUMBER) as    CAN,a.COMPANY_NUMBER,a.ACCOUNT_NUMBER,a.SUBSCRIBER_SEQ,b.ban_num,a.PRIMARY_OR_DEPENDANT,   
# MAGIC a.CYCLE_NUMBER,a.STATUS,b.ORIGINAL_CONNECTION_DATE from MARQUEE.Account a    
# MAGIC left join MARQUEE.Subscriber b on a.SUBSCRIBER_SEQ=b.SUBSCRIBER_SEQ    
# MAGIC left join marquee.subscriber_address c on a.SUBSCRIBER_SEQ=c.SUBSCRIBER_SEQ and c.LATEST_ADDRESS_INDICATOR in ('Y')    
# MAGIC left join marquee.address d on c.ADDRESS_SEQ=d.ADDRESS_SEQ where a.STATUS in ('1','2','3','4') and d.CONTRACT_GROUP_CODE in ('1','3','4')) MQ left join (select distinct BAN from ods.billing_account where LIVE_NONLIVE='Y' ) OD on MQ.BAN_num=OD.BAN    
# MAGIC left join hash_lookup.ecid_ban_can hash on MQ.CAN = hash.account"""    
# MAGIC 
# MAGIC ##iginte twoYearStart  is  2020-06-21   twoYearEnd  is  2022-06-18    
# MAGIC 
# MAGIC query_ignite_base="""select ACCOUNT_ID,ACCOUNT_STATUS,AR_ACCOUNT_SUB_TYPE,AR_ACCOUNT_TYPE,X_LOB,x_acquision_date,x_start_date,   
# MAGIC WIRELESS_ACCOUNT,EC_ID,X_CNSLD_ACCNT_ID,BRAND,c.hash_account from app_maestro.acctdim A    
# MAGIC INNER JOIN (select DISTINCT customer_account from app_ibro.ibro_household_closing    
# MAGIC where report_date between to_date('"""+twoYearStart+"""') and to_date('"""+twoYearEnd+"""') and    
# MAGIC multi_system like '%MS%' AND MULTI_BRAND LIKE '%ROGERS%' and multi_product_name like '%TV%INT%') B ON A.ACCOUNT_ID=B.customer_account    
# MAGIC left join hash_lookup.ecid_ban_can c on a.account_id=c.account where A.maestro_ind='Y' and A.crnt_f='Y'"""   

# COMMAND ----------

#Legacy base query 
git_leg_base_1=spark.sql("select * from hive_metastore.mlmodel_data.git_original_legacy_base")
git_leg_base_2=git_leg_base_1.select(col("`mq.can`").alias("ban_can"), col("`hash.hash_account`").alias("hash_ban_can"), col("`hash.ecid`").alias("ecid"), col("`mq.company_number`").alias("CUSTOMER_COMPANY"), col("`mq.original_connection_date`").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer'))).withColumn("type",lit("Legacy"))
display(git_leg_base_2)

# COMMAND ----------

git_leg_base_1_compare=spark.sql("select * from hive_metastore.default.git_original_base_legacy2")
git_leg_base_2_compare=git_leg_base_1_compare.select(col("`mq.can`").alias("ban_can"), col("`hash.hash_account`").alias("hash_ban_can"), col("`hash.ecid`").alias("ecid"), col("`mq.company_number`").alias("CUSTOMER_COMPANY"), col("`mq.original_connection_date`").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer'))).withColumn("type",lit("Legacy"))
display(git_leg_base_2_compare)

# COMMAND ----------

#Ignite base query 
git_ignite_base_1=spark.sql("select * from hive_metastore.mlmodel_data.git_original_ignite_base") 
git_ignite_base_2=git_ignite_base_1.select(col("account_id").alias("ban_can"), col("`c.hash_account`").alias("hash_ban_can"), col("ec_id").alias("ecid"), col("x_acquision_date").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer'))).withColumn("type",lit("Ignite")).withColumn("CUSTOMER_COMPANY",lit(None))
git_ignite_base_2=git_ignite_base_2.select("ban_can","hash_ban_can","ecid","CUSTOMER_COMPANY","tenure_dt","tenure_month","type")
display(git_ignite_base_2)

# COMMAND ----------

#Ignite base query 
git_ignite_base_1_compare=spark.sql("select * from hive_metastore.default.git_original_base_ignite2") 
git_ignite_base_2_compare=git_ignite_base_1_compare.select(col("account_id").alias("ban_can"), col("`c.hash_account`").alias("hash_ban_can"), col("ec_id").alias("ecid"), col("x_acquision_date").alias("tenure_dt")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer'))).withColumn("type",lit("Ignite")).withColumn("CUSTOMER_COMPANY",lit(None))
git_ignite_base_2_compare=git_ignite_base_2_compare.select("ban_can","hash_ban_can","ecid","CUSTOMER_COMPANY","tenure_dt","tenure_month","type")
display(git_ignite_base_2_compare)

# COMMAND ----------

print("Merge legacy and Ignite")
git_merged_base_1 = git_leg_base_2.union(git_ignite_base_2)   
print("Deduplicate") 
git_merged_base_2=git_merged_base_1.withColumn("row_num", row_number().over(Window.partitionBy("ecid").orderBy(asc("type")))).where(col("ecid").isNotNull()).groupby("ecid").agg(F.max("tenure_month").alias('tenure_month'),F.max(when(col("row_num") == 1,col("type")).otherwise(None)).alias('type'),F.max(when(col("row_num") == 1,col("CUSTOMER_COMPANY")).otherwise(None)).alias('CUSTOMER_COMPANY')).drop("row_num")
print("total_count:", git_merged_base_2.count())
print(git_merged_base_2.groupby("type").count().show())
display(git_merged_base_2)
#2127671

# COMMAND ----------

print("Merge legacy and Ignite") # for updated hadoop data
git_merged_base_1_compare = git_leg_base_2_compare.union(git_ignite_base_2_compare)   
print("Deduplicate") 
git_merged_base_2_compare=git_merged_base_1_compare.withColumn("row_num", row_number().over(Window.partitionBy("ecid").orderBy(asc("type")))).where(col("ecid").isNotNull()).groupby("ecid").agg(F.max("tenure_month").alias('tenure_month'),F.max(when(col("row_num") == 1,col("type")).otherwise(None)).alias('type'),F.max(when(col("row_num") == 1,col("CUSTOMER_COMPANY")).otherwise(None)).alias('CUSTOMER_COMPANY')).drop("row_num")
print("total_count:", git_merged_base_2_compare.count())
print(git_merged_base_2_compare.groupby("type").count().show())
display(git_merged_base_2_compare)
#2127671->2050012

# COMMAND ----------

df1_leg_base_2_attr_filtered=df1_leg_base_1_attr_filtered.select(col("CAN").alias("ban_can"),col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE").alias("tenure_dt"),col("PRODUCT_SOURCE"),col("CUSTOMER_COMPANY")).withColumn("tenure_month",abs(months_between(to_date("tenure_dt"), current_date()).cast('integer')))
print("Deduplicate") 
#only count once for each ecid, using same logic with github prefer keeping ignite and first row company number
df1_merged_base_2_attr_filtered=df1_leg_base_2_attr_filtered.withColumn("row_num", row_number().over(Window.partitionBy("ecid").orderBy(asc("PRODUCT_SOURCE")))).where(col("ecid").isNotNull()).groupby("ecid").agg(F.max("tenure_month").alias('tenure_month'),F.max(when(col("row_num") == 1,col("PRODUCT_SOURCE")).otherwise(None)).alias('PRODUCT_SOURCE'),F.max(when(col("row_num") == 1,col("CUSTOMER_COMPANY")).otherwise(None)).alias('CUSTOMER_COMPANY')).drop("row_num").cache()
print(df1_merged_base_2_attr_filtered.count())#1957015 
print(df1_merged_base_2_attr_filtered.groupby("PRODUCT_SOURCE").count().show())
display(df1_merged_base_2_attr_filtered)

# COMMAND ----------

print("******git_merged_base_2********")
print("total_count:", git_merged_base_2.count())
print(git_merged_base_2.groupby("type").count().show())
print("******git_merged_base_2_compare********")
print("total_count:", git_merged_base_2_compare.count())
print(git_merged_base_2_compare.groupby("type").count().show())
print("**************")
#MS means ignite, SS means legacy
print(df1_merged_base_2_attr_filtered.count())#1957015 
print(df1_merged_base_2_attr_filtered.groupby("PRODUCT_SOURCE").count().show())

#even if add with same logic for choosing product source(ignite/legacy) when only keeping one ecid row for each ecid,
# the sources simpliy don't match, don't know why is that, if so, then 'product source' is unreliable for discrepancy source investigation
# the same problem with "customer_company", when null, means it must be 'ignite'
print("*******git_merged_base_2**********")
print(git_merged_base_2.groupby("CUSTOMER_COMPANY").count().show())
print("********git_merged_base_2_compare******")
print(git_merged_base_2_compare.groupby("CUSTOMER_COMPANY").count().show())
print("*****************")       
print(df1_merged_base_2_attr_filtered.groupby("CUSTOMER_COMPANY").count().show())
print("*****************")       
           


# COMMAND ----------

display(df1_merged_base_2_attr_filtered)

# COMMAND ----------

df1_merged_base_2_attr_filtered.createOrReplaceTempView("attr_filtered_merged_base_2")
git_merged_base_2.createOrReplaceTempView("git_merged_base_2")
#git_merged_base_2_compare.createOrReplaceTempView("git_merged_base_2_compare")#updated hadoop data

# COMMAND ----------

#keep records of ecid which are in github code but not our system
git_duo_ecid=spark.sql("""
select distinct ecid from git_merged_base_2 where git_merged_base_2.ecid not in (select distinct ecid from attr_filtered_merged_base_2)
""")
print(git_duo_ecid.count())
git_duo_ecid.show(10)
#meaning 751,084 out of the 2,127,671(almost one third of the ecid are in git but not in our system

our_system_duo_ecid=spark.sql("""
select distinct ecid from attr_filtered_merged_base_2 where attr_filtered_merged_base_2.ecid not in (select distinct ecid from git_merged_base_2)
""")
print(our_system_duo_ecid.count())
our_system_duo_ecid.show(10)
#meaning 580,428 out of the 1957015(almost one third of the ecid are in our system but not in git
same_occur_ecid=spark.sql("""
select distinct ecid from attr_filtered_merged_base_2 where attr_filtered_merged_base_2.ecid in (select distinct ecid from git_merged_base_2)
""")
print(same_occur_ecid.count())
#dicrepency could arise from two sources, the model or the input,
# what i was doing yesterday is trying to validate the sources, 
#1376587 only 65%-69% ecid (identifier/data) overlap between their system and ours, distribution shift could happen when you have input of this much difference, 
#so this is my hypothesis, I think we have input/data source problems.
#Under this scenario, if i still want to validate the model, what i could do is to only look at the data at overlap,
#and check their output prediction distribution (in other words, using same input, compare the outputs)


# COMMAND ----------

#rogers overlapping ecid same_occur_ecid

schema='mlmodel_data'
model_no='123_yuncheng' 
print("writing overlapping rogers base customer ecid into schema " + schema +".DAT1_"+model_no+"_rogers_overlap")
same_occur_ecid.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no+"_rogers_overlap")
print(schema+".DAT1_"+model_no+"_rogers_overlap generated successfully")

# COMMAND ----------

#keep records of ecid which are in github code but not our system
git_duo_ecid_compare=spark.sql("""
select distinct ecid from git_merged_base_2_compare where git_merged_base_2_compare.ecid not in (select distinct ecid from attr_filtered_merged_base_2)
""")
print(git_duo_ecid_compare.count())
git_duo_ecid_compare.show(10)
#meaning 361055 out of the 2050012(almost one seventh of the ecid are in git but not in our system

our_system_duo_ecid_compare=spark.sql("""
select distinct ecid from attr_filtered_merged_base_2 where attr_filtered_merged_base_2.ecid not in (select distinct ecid from git_merged_base_2_compare)
""")
print(our_system_duo_ecid_compare.count())
our_system_duo_ecid_compare.show(10)
#meaning 268058 out of the 1957015(almost one eighth of the ecid are in our system but not in git
same_occur_ecid_compare=spark.sql("""
select distinct ecid from attr_filtered_merged_base_2 where attr_filtered_merged_base_2.ecid in (select distinct ecid from git_merged_base_2_compare)
""")
print(same_occur_ecid_compare.count())
#dicrepency could arise from two sources, the model or the input,
# what i was doing yesterday is trying to validate the sources, 
#1688957 only 82%-86% ecid (identifier/data) overlap between their system and ours, distribution shift could happen when you have input of this much difference, 
#so this is my hypothesis, I think we have input/data source problems.
#Under this scenario, if i still want to validate the model, what i could do is to only look at the data at overlap,
#and check their output prediction distribution (in other words, using same input, compare the outputs)


# COMMAND ----------



# COMMAND ----------

df1_merged_base_2_attr_filtered.select("ECID","tenure_month")

schema='mlmodel_data'
model_no_r='123_yuncheng' 
print("writing merged Rogers base customer query into schema " + schema)
df1_merged_base_2.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT1_"+model_no_r+"_r_customer_base")
print(schema+".DAT1_"+model_no_r+"_r_customer_base generated successfully")

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
# MAGIC #fiddo

# COMMAND ----------

 query_fido_base="""select a.account_id  ,b.account_status ,b.ar_account_sub_type ,b.ar_account_type ,b.x_conv_ind ,b.x_hh_id ,b.x_lob ,b.wireless_account ,b.ec_id ,b.x_acquision_date ,b.contact_key ,b.x_is_cnsld ,b.x_cnsld_accnt_id ,b.lob_ind ,b.account_status_code ,b.brand ,c.hash_account from ( select distinct account_id from app_maestro.Vw_PntrDly where product_status_code in ('AC')  and calendar_date ='"""+twoYearEnd+"""' and brand='Fido' and prod_hshld_key='15002' ) a inner join app_maestro.acctdim b on a.account_id = b.account_id and b.crnt_f='Y' and b.maestro_ind='Y' left join hash_lookup.ecid_ban_can c on a.account_id = c.account"""
        


# COMMAND ----------

# MAGIC %md
# MAGIC select a.account_id ,b.account_status ,b.ar_account_sub_type ,b.ar_account_type ,b.x_conv_ind ,b.x_hh_id ,b.x_lob ,b.wireless_account ,b.ec_id ,b.x_acquision_date ,b.contact_key ,b.x_is_cnsld ,b.x_cnsld_accnt_id ,b.lob_ind ,b.account_status_code ,b.brand ,c.hash_account    from ( select distinct account_id from app_maestro.Vw_PntrDly    
# MAGIC where product_status_code in ('AC')  and calendar_date ='2022-06-18' and brand='Fido' and prod_hshld_key='15002' ) a    
# MAGIC inner join app_maestro.acctdim b on a.account_id = b.account_id and b.crnt_f='Y' and b.maestro_ind='Y'     
# MAGIC left join hash_lookup.ecid_ban_can c on a.account_id = c.account

# COMMAND ----------

#hadoop original query

git_query_fido_base_1_compare=spark.sql("select * from hive_metastore.mlmodel_data.git_query_fido_base") 
display(git_query_fido_base_1_compare)

# COMMAND ----------

git_query_fido_base_2_compare=git_query_fido_base_1_compare.select(col("`a.account_id`").alias("ban_can"),col("`c.hash_account`").alias("hash_ban_can"),col("`b.ec_id`").alias("ECID"),col("`b.x_acquision_date`")).withColumn("tenure_month",abs(months_between(to_date("`b.x_acquision_date`"), current_date()).cast('integer'))).where(col("ECID").isNotNull()).groupby("ECID").agg(F.max("tenure_month").alias('tenure_month'))
display(git_query_fido_base_2_compare)
print(git_query_fido_base_2_compare.count())#228558

# COMMAND ----------

#landed --app_maestro.Vw_PntrDly-- APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST into azure

df1_query_fido_base_1=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Fido' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and ATTR_ADDRESS_SERVICEBILITY = 'P' and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd))
# APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST
# ATTR_ADDRESS_SERVICEBILITY = 'P'
# tenure_month supposed to be all zero
df1_query_fido_base_2=df1_query_fido_base_1.select(col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE")).withColumn("tenure_month",abs(months_between(to_date("ACTIVITY_DATE"), current_date()).cast('integer'))).where(col("ECID").isNotNull()).groupby("ECID").agg(F.max("tenure_month").alias('tenure_month'))
print(df1_query_fido_base_2.count())
print(df1_query_fido_base_2.select('ECID').distinct().count())
print("*******************")

df1_query_fido_base_1_attr_filtered=spark.sql(f"select CUSTOMER_ID,CUSTOMER_ACCOUNT,CUSTOMER_COMPANY,case when CUSTOMER_COMPANY is not NULL then trim(concat(CUSTOMER_COMPANY,CUSTOMER_ACCOUNT))else CUSTOMER_ACCOUNT end as CAN,ENTERPRISE_ID as ECID,CR_Key,CUSTOMER_LOCATION_ID,CUSTOMER_MULTI_PRODUCT,PRODUCT_SOURCE,ACTIVITY_DATE FROM APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_HIST WHERE  PRODUCT_BRAND = 'Fido' and CUSTOMER_SEGMENT ='CBU' and ACTIVITY = 'CL' and  ATTR_ADDRESS_CONTRACT_GROUP in ('1', '3','4') and  ACTIVITY_DATE  ='{twoYearEnd}'".format(twoYearEnd))
df1_query_fido_base_2_attr_filtered=df1_query_fido_base_1_attr_filtered.select(col("CUSTOMER_ID").alias("hash_ban_can"),col("ECID").alias("ecid"),col("ACTIVITY_DATE")).withColumn("tenure_month",abs(months_between(to_date("ACTIVITY_DATE"), current_date()).cast('integer'))).where(col("ECID").isNotNull()).groupby("ECID").agg(F.max("tenure_month").alias('tenure_month'))
print(df1_query_fido_base_2_attr_filtered.count())
print(df1_query_fido_base_2_attr_filtered.select('ECID').distinct().count())
#222404->227336

# COMMAND ----------

df1_query_fido_base_2_attr_filtered.createOrReplaceTempView("df1_query_fido_base_2_attr_filtered")
df1_query_fido_base_2.createOrReplaceTempView("df1_query_fido_base_2")
git_query_fido_base_2_compare.createOrReplaceTempView("git_query_fido_base_2_compare")#updated hadoop data

# COMMAND ----------

#keep records of ecid which are in github code but not our system
git_duo_ecid_fido=spark.sql("""
select distinct ECID from git_query_fido_base_2_compare where git_query_fido_base_2_compare.ECID not in (select distinct ECID from df1_query_fido_base_2_attr_filtered)
""")
print(git_duo_ecid_fido.count())
git_duo_ecid_fido.show(10)
#meaning 3393 out of the 228558 of the ecid are in git but not in our system

our_system_duo_ecid_fido=spark.sql("""
select distinct ECID from df1_query_fido_base_2_attr_filtered where df1_query_fido_base_2_attr_filtered.ECID not in (select distinct ECID from git_query_fido_base_2_compare)
""")
print(our_system_duo_ecid_fido.count())
our_system_duo_ecid_fido.show(10)
#meaning 2171 out of the 227336 of the ecid are in our system but not in git
same_occur_ecid_fido=spark.sql("""
select distinct ECID from df1_query_fido_base_2_attr_filtered where df1_query_fido_base_2_attr_filtered.ECID in (select distinct ECID from git_query_fido_base_2_compare)
""")
print(same_occur_ecid_fido.count())
#225165 98.5%-99% ecid (identifier/data) overlap between their system and ours 


# COMMAND ----------

#keep records of ecid which are in github code but not our system
git_duo_ecid_fido_original=spark.sql("""
select distinct ECID from git_query_fido_base_2_compare where git_query_fido_base_2_compare.ECID not in (select distinct ECID from df1_query_fido_base_2)
""")
print(git_duo_ecid_fido_original.count())
git_duo_ecid_fido_original.show(10)
#meaning 3393 out of the 228558 of the ecid are in git but not in our system

our_system_duo_ecid_fido_original=spark.sql("""
select distinct ECID from df1_query_fido_base_2 where df1_query_fido_base_2.ECID not in (select distinct ECID from git_query_fido_base_2_compare)
""")
print(our_system_duo_ecid_fido_original.count())
our_system_duo_ecid_fido_original.show(10)
#meaning 2103 out of the 222404 of the ecid are in our system but not in git
same_occur_ecid_fido_original=spark.sql("""
select distinct ECID from df1_query_fido_base_2 where df1_query_fido_base_2.ECID in (select distinct ECID from git_query_fido_base_2_compare)
""")
print(same_occur_ecid_fido_original.count())
#220301 96%-99% ecid (identifier/data) overlap between their system and ours 


# COMMAND ----------


