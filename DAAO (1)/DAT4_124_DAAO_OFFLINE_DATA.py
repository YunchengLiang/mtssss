# Databricks notebook source
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import traceback
import logging
from pyspark.sql.functions import date_format
import pyspark.sql.functions as F
from pyspark.sql.functions import datediff, to_date, lit
from pyspark.sql.functions import *
import dateutil.relativedelta

# COMMAND ----------

offset = ((datetime.today().weekday() - 6) % 7)
twoYearEnd = ((datetime.today() - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")
twoYearStart_tmp=(( datetime.today()+ dateutil.relativedelta.relativedelta(weeks=-104)))
offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)
twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")
OpenYrMth = datetime.today().strftime("%y"+"%m"+"%d")
sixMoStart_tmp = datetime.today()+ dateutil.relativedelta.relativedelta(months=-6)
offset1 = ((sixMoStart_tmp.weekday() - 6) % 7)
sixMoStart = (sixMoStart_tmp - timedelta(days=offset1)).strftime("%Y-%m-%d")
print("OpenYrMth  is ", OpenYrMth)
print("twoYearStart  is ", twoYearStart)
print("twoYearEnd  is ", twoYearEnd)
print("six months ago  is ",sixMoStart)


# COMMAND ----------

test=spark.sql("select * from app_cbc.cbc_icm_mthly")
#use hash table to get ECIDs
print(test.columns)
print(test.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted app_cbc.cbc_icm_mthly

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct *) from hash_lookup.ecid_ban_can

# COMMAND ----------

table1= spark.sql("select ECID as ec_id,ACCOUNT from hash_lookup.ecid_ban_can")
table2= spark.sql("select * from app_cbc.cbc_icm_mthly")

test_df= table1.join(table2,table1["ACCOUNT"] == table2["BAN"])
display(test_df)
test_df.createOrReplaceTempView("test_df")

# COMMAND ----------

#/*Offline transactions*/
#/*Fido internet ICM transactions*/
schema='mlmodel_data'
model_no= 'test_sg_f'
#TODO: app_cbc.cbc_icm_mthly has no ecid
query_fido="""select * from test_df b inner join (select distinct ecid from """+schema+""".DAT1_"""+model_no+"""_f_customer_base) a on a.ecid = b.ec_id where b.medium_cd <> 'WEB' and b.direction = 1 and b.channel_cd <> 'ROGERS.COM' and b.blg_cust_type in ('Consumer','Fido Consommateur','Fido Consumer') and b.blg_cust_sub_type in ('Regular','Standard') and b.src_cd = 'C' and to_date(b.intrxn_start_time) >= to_date('"""+twoYearStart+"""') and to_date(b.intrxn_start_time) <= to_date('"""+twoYearEnd+"""')"""


df_query=spark.sql(query_fido)
display(df_query)
print("Fido Offline -------------------->",query_fido)

# COMMAND ----------


#/*Fido internet ICM transactions*/


df_query.createOrReplaceTempView("query_fido")
#replaced intrxn_start_dt with INTRXN_START_TIME
query_fido_cleansed="""select a.ecid, INTRXN_START_TIME as intrxn_start_dt, case when icm_retail_flag in ('y','Y') then 'Retail' when icm_retail_flag in ('n','N') and channel_cd in ('CALL CENTERS','CALL CENTRES','SUPPORT GROUP','NOVA','GENESYSESERVICES','GENESYSDIALER') then 'Care' when icm_retail_flag in ('n','N') and channel_cd in ('IVR') then 'IVR' when icm_retail_flag in ('n','N') and channel_cd in ('RETAIL','DEALERS','retail','UTE SMB RETAIL') then 'Retail' when icm_retail_flag in ('n','N') and channel_cd in ('FACEBOOK','CAM','CMS','TWITTER') then 'Digital'  else 'NA' end as channel_group,  case when reason_1 in ('Billing') then 'Use' when reason_1 in ('Payment and Collections') then 'Pay' when reason_1 in ('Products, Services and Ordering','Technical/Product Support','Account Maintenance','Products Services and Ordering','Pilot Use Only','Hardware Delivery and Management','Number Management','Accessibility','Equipment Maintenance') then 'Support' else 'NA' end as lbgups  from query_fido a where login_name <> 'sa' and channel_cd <> 'FIDO.CA'"""
        
print("Cleansed offline",query_fido_cleansed)		
df4_2_fido=spark.sql(query_fido_cleansed)



# COMMAND ----------

display(df4_2_fido)

# COMMAND ----------

#/*CJA data - Fido*/


query_cja="""select to_date(concat(year,'-',month,'-',day)) as trans_date, a.ecid, count(*) as total_offline_events, (case when (trans_channel_level_1 = 'Call Center' and (call_direction is null or call_direction = '1')) then 'Call' when trans_channel_level_1 = 'Retail' then 'Retail' when trans_channel_level_1 = 'IVR' then 'IVR' when trans_channel_level_1 = 'eChat' then 'Chat' end) as channel, 'Offline' as channel_type, sum(case when lbgups like '%Learn%' then 1 else 0 end) as offline_learn, sum(case when lbgups like '%Buy%' then 1 else 0 end) as offline_buy, sum(case when lbgups like '%Get%' then 1 else 0 end) as offline_get, sum(case when lbgups like '%Use%' then 1 else 0 end) as offline_use, sum(case when lbgups like '%Pay%' then 1 else 0 end) as offline_pay, sum(case when lbgups like '%Support%' then 1 else 0 end) as offline_support, sum(case when lbgups like '%Learn%' and available_online=1 then 1 else 0 end) as offline_learn_online, sum(case when lbgups like '%Buy%' and available_online=1  then 1 else 0 end) as offline_buy_online, sum(case when lbgups like '%Get%' and available_online=1  then 1 else 0 end) as offline_get_online, sum(case when lbgups like '%Use%' and available_online=1  then 1 else 0 end) as offline_use_online, sum(case when lbgups like '%Pay%' and available_online=1  then 1 else 0 end) as offline_pay_online, sum(case when lbgups like '%Support%' and available_online=1  then 1 else 0 end) as offline_support_online from app_customer_journey.vw_cbu_cust_journey_fct a inner join (select distinct ecid from """+schema+""".DAT1_"""+model_no+"""_f_customer_base) c on a.ecid=c.ecid left join omniture.offline_txn_map b on a.event_description = b.event where to_date(concat(year,'-',month,'-',day)) >= to_date('"""+twoYearStart+"""') and to_date(concat(year,'-',month,'-',day)) < to_date('"""+twoYearEnd+"""') and ((trans_channel_level_1 = 'Call Center' and (call_direction is null or call_direction = '1')) or trans_channel_level_1 in ('Retail', 'IVR', 'eChat')) and franchise = 'F' group by to_date(concat(year,'-',month,'-',day)), a.ecid, (case when (trans_channel_level_1 = 'Call Center' and (call_direction is null or call_direction = '1')) then 'Call' when trans_channel_level_1 = 'Retail' then 'Retail' when trans_channel_level_1 = 'IVR' then 'IVR' when trans_channel_level_1 = 'eChat' then 'Chat' end)"""
print("CJA Fido",query_cja)
df4_3_cja=spark.sql(query_cja)
display(df4_3_cja)

# COMMAND ----------

#/*Combine CJA and fido txns*/
df4_merged=df4_3_cja.select("trans_date","ecid","total_offline_events","channel","channel_type","offline_learn","offline_buy","offline_get","offline_use","offline_pay","offline_support")\
           .union(df4_2_fido.withColumn("channel",when (upper(col("channel_group")).isin(['CARE','IVR']),"Care").otherwise(col("channel_group")))\
           .withColumn("channel_type",lit('Offline'))\
           .withColumn("offline_learn",when(col("lbgups").like("%Learn%"),1).otherwise(0))\
           .withColumn("offline_buy",when(col("lbgups").like("%Buy%"),1).otherwise(0))\
           .withColumn("offline_get",when(col("lbgups").like("%Get%"),1).otherwise(0))\
           .withColumn("offline_use",when(col("lbgups").like("%Use%"),1).otherwise(0))\
           .withColumn("offline_pay",when(col("lbgups").like("%Pay%"),1).otherwise(0))\
           .withColumn("offline_support",when(col("lbgups").like("%Support%"),1).otherwise(0))\
           .groupby('intrxn_start_dt','ecid','channel','channel_type')\
           .agg(sum('offline_learn').alias("offline_learn"),
                 sum('offline_buy').alias("offline_buy"),
                 sum('offline_get').alias("offline_get"),
                 sum('offline_use').alias("offline_use"),
                 sum('offline_pay').alias("offline_pay"),
                 sum('offline_support').alias("offline_support"),
                 count(lit(1)).alias("total_offline_events")).selectExpr("intrxn_start_dt as trans_date","ecid as ecid","total_offline_events","channel","channel_type","offline_learn","offline_buy","offline_get","offline_use","offline_pay","offline_support"))




# COMMAND ----------

df4_merged.createOrReplaceTempView("df4_merged")
print("Merge completed")

#/*Add sales flag to offline data*/

query_sales_flg="""select trans_date, a.ecid, a.channel, a.channel_type,
        avg(total_offline_events) as total_offline_events,
        avg(offline_learn) as offline_learn,
        avg(offline_buy) as offline_buy,
        avg(offline_get) as offline_get,
        avg(offline_use) as offline_use,
        avg(offline_pay) as offline_pay,
        avg(offline_support) as offline_support,
        sum(case when trans_date between date_sub(sale.`date`,30) and sale.`date` then sales_vol end) as sales_vol
from df4_merged a
left join (select `date`, ecid, sum(sales_vol) as sales_vol from """+schema+""".DAT2_"""+model_no+"""_f_sls_summary
            group by `date`, ecid) sale on a.ecid = sale.ecid
group by trans_date,  a.ecid,  a.channel, a.channel_type"""

print("Writing data",query_sales_flg)
df4_final=spark.sql(query_sales_flg)


df4_final.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT4_"+model_no+"_f_offline_data")
print("table "+schema+".DAT4_"+model_no+"_f_offline_data generated successfully")



# COMMAND ----------

display(df4_final)

# COMMAND ----------

df4_final.groupby('sales_vol').count().show()

# COMMAND ----------


