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

today=datetime(2022,6,21)
offset = ((today.weekday() - 6) % 7)#monday->1 sunday->7 
twoYearEnd = ((today - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")#41 date of last saturday?
twoYearStart_tmp=((today+ dateutil.relativedelta.relativedelta(weeks=-104)))#41 2 years before today
offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)# weekday of "2 years before today"
twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")# the last sunday before "2 years before today"
OpenYrMth = today.strftime("%y"+"%m"+"%d")#today
sixMoStart_tmp = today + dateutil.relativedelta.relativedelta(months=-6)#6 month before today
offset1 = ((sixMoStart_tmp.weekday() - 6) % 7)# weekday of '6 month before today'
sixMoStart = (sixMoStart_tmp - timedelta(days=offset1)).strftime("%Y-%m-%d") # last sunday before '6 month before today'
print("OpenYrMth  is ", OpenYrMth)
print("twoYearStart  is ", twoYearStart)
print("twoYearEnd  is ", twoYearEnd)
print("six months ago  is ",sixMoStart)

#offset = ((datetime.today().weekday() - 6) % 7)
#twoYearEnd = ((datetime.today() - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")
#twoYearStart_tmp=(( datetime.today()+ dateutil.relativedelta.relativedelta(weeks=-104)))
#offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)
#twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")
#OpenYrMth = datetime.today().strftime("%y"+"%m"+"%d")
#sixMoStart_tmp = datetime.today()+ dateutil.relativedelta.relativedelta(months=-6)
#offset1 = ((sixMoStart_tmp.weekday() - 6) % 7)
#sixMoStart = (sixMoStart_tmp - timedelta(days=offset1)).strftime("%Y-%m-%d")
#print("OpenYrMth  is ", OpenYrMth)
#print("twoYearStart  is ", twoYearStart)
#print("twoYearEnd  is ", twoYearEnd)
#print("six months ago  is ",sixMoStart)
#41 question, what is the sixmonths-tmp used for 



# import datetime
# today = datetime.datetime(2020, 7, 21)
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

#/*Offline transactions*/
#/*Ignite transactions*/

#app_maestro.asgndprodtxngrfct    app_maestro.acctdim   app_maestro.proddim
#base table as filter for ecid
schema='mlmodel_data'
model_no= '123_yuncheng'
query_ignite_1="""select distinct a.PRODUCT_KEY ,a.CUSTOMER_KEY ,a.SERVICE_TYPE ,a.MAIN_ITEM_ID ,a.COMM_EMPLOYEE_ID ,a.WORK_ORDER_ID ,a.ITEM_ACTION_TYPE ,a.ORDER_ID ,a.ORDER_MODE ,a.ORDER_STATUS ,a.ORDER_CREATION_DATE ,a.ORDER_ACTION_TYPE ,a.ORDER_ACTION_STATUS ,a.ORDER_ACTION_CREATION_DATE ,a.SELLING_CHANNEL_CODE ,a.SELLING_CHANNEL_DESC ,a.BRAND ,a.SELLING_CHANNEL ,a.EMPUSER_ID ,a.DEALER_CODE ,b.EC_ID as ECID ,c.product_src ,c.lev_1_desc ,c.lev_2_desc ,c.lev_3_desc ,c.lev_4_desc ,c.Prod_Desc ,c.Prod_Caption ,c.PROD_SUB_TYPE_CD ,c.PROD_SUB_TYPE_CD_DESC ,c.ENT_type ,c.LOB_Desc ,c.Group_ID ,c.Group_Desc from app_maestro.asgndprodtxngrfct  a inner join app_maestro.acctdim  b on a.CUSTOMER_KEY=b.CUSTOMER_KEY inner join """+schema+""".DAT1_"""+model_no+"""_r_customer_base base on base.ecid=b.ec_id left join (select product_key ,product_src ,lev_1_desc ,lev_2_desc ,lev_3_desc ,lev_4_desc ,Prod_Desc ,Prod_Caption ,PROD_SUB_TYPE_CD ,PROD_SUB_TYPE_CD_DESC ,ENT_type ,LOB_Desc ,Group_ID ,Group_Desc from app_maestro.proddim where CRNT_F='Y') c on a.product_key=c.product_key WHERE a.assigned_prod_status IN ('AC') and a.Final_Status='AC' AND a.ORDER_STATUS='DO' and a.ORDER_ACTION_STATUS='DO' and a.DUMMY_ADDRESS_IND='N' and ORDER_CREATION_DATE >= to_date('"""+twoYearStart+"""')  and ORDER_CREATION_DATE <=to_date('"""+twoYearEnd+"""')  and a.Brand='Rogers' and a.ORDER_ACTION_TYPE='CH'"""

df_query=spark.sql(query_ignite_1)
print("Ignite Offline -------------------->",query_ignite_1)

df_query.createOrReplaceTempView("query_ignite")

#/*Ignite txns clean up*/


query_ignite_cleansed="""select ecid, cast(order_creation_date as date) as trans_date, count(*) as total_offline_events, (CASE  WHEN upper(SELLING_CHANNEL)='SSP' and upper(DEALER_CODE) = 'UTECONSUMER' or selling_channel = 'UTERogersOnline' THEN 'ROGERS.COM' WHEN upper(SELLING_CHANNEL)='SSP' AND upper(DEALER_CODE) LIKE '%ONEVIEW%' THEN 'Retail' WHEN upper(SELLING_CHANNEL)='CARE' or selling_channel = 'UTERogersCare' or selling_channel = 'ONEVIEW' THEN 'CARE' ELSE 'Other' END) AS CHANNEL, sum(case when lbgups like '%Learn%' then 1 else 0 end) as offline_learn, sum(case when lbgups like '%Buy%' then 1 else 0 end) as offline_buy, sum(case when lbgups like '%Get%' then 1 else 0 end) as offline_get, sum(case when lbgups like '%Use%' then 1 else 0 end) as offline_use, sum(case when lbgups like '%Pay%' then 1 else 0 end) as offline_pay, sum(case when lbgups like '%Support%' then 1 else 0 end) as offline_support from query_ignite a left join omniture.offline_txn_map b on  a.prod_sub_type_cd_desc = b.event  where lbgups is not null and lbgups <> 'Get' and (upper(SELLING_CHANNEL||'-'||DEALER_CODE)<>'SSP-UTECONSUMER') and selling_channel <> 'UTERogersOnline' and selling_channel <> 'Oasys' and ecid is not null group by 1,2,4 order by 3 desc"""


print("Ignite txns clean up ---------------------->",query_ignite_cleansed)
df4_2_ignite=spark.sql(query_ignite_cleansed)
display(df4_2_ignite)


# COMMAND ----------

test=spark.sql("select ecid from mlmodel_data.DAT1_123_yuncheng_r_customer_base base")
display(test)

# COMMAND ----------

# app_maestro.acctdim
test2=spark.sql("select ec_id,customer_key from app_maestro.acctdim")
test3=test2.join(test,test2.ec_id==test.ecid,'inner')
display(test3)
#check if they have inner join, test and test2 on ec_id

# COMMAND ----------

#/*CJA data - Rogers and Cable*/
#app_customer_journey.vw_cbu_cust_journey_fct  omniture.offline_txn_map
query_cja="""select to_date(concat(year,'-',month,'-',day)) as trans_date, a.ecid, count(*) as total_offline_events, (case when (trans_channel_level_1 = 'Call Center' and (call_direction is null or call_direction = '1')) then 'Call' when trans_channel_level_1 = 'Retail' then 'Retail' when trans_channel_level_1 = 'IVR' then 'IVR' when trans_channel_level_1 = 'eChat' then 'Chat' end) as channel, 'Offline' as channel_type, sum(case when lbgups like '%Learn%' then 1 else 0 end) as offline_learn, sum(case when lbgups like '%Buy%' then 1 else 0 end) as offline_buy, sum(case when lbgups like '%Get%' then 1 else 0 end) as offline_get, sum(case when lbgups like '%Use%' then 1 else 0 end) as offline_use, sum(case when lbgups like '%Pay%' then 1 else 0 end) as offline_pay, sum(case when lbgups like '%Support%' then 1 else 0 end) as offline_support, sum(case when lbgups like '%Learn%' and available_online=1 then 1 else 0 end) as offline_learn_online, sum(case when lbgups like '%Buy%' and available_online=1  then 1 else 0 end) as offline_buy_online, sum(case when lbgups like '%Get%' and available_online=1  then 1 else 0 end) as offline_get_online, sum(case when lbgups like '%Use%' and available_online=1  then 1 else 0 end) as offline_use_online, sum(case when lbgups like '%Pay%' and available_online=1  then 1 else 0 end) as offline_pay_online, sum(case when lbgups like '%Support%' and available_online=1  then 1 else 0 end) as offline_support_online from app_customer_journey.vw_cbu_cust_journey_fct a inner join (select distinct ecid from """+schema+""".DAT1_"""+model_no+"""_r_customer_base) c on a.ecid=c.ecid left join  omniture.offline_txn_map b on a.event_description = b.event where to_date(concat(year,'-',month,'-',day)) >= to_date('"""+twoYearStart+"""') and to_date(concat(year,'-',month,'-',day)) < to_date('"""+twoYearEnd+"""') and ((trans_channel_level_1 = 'Call Center' and (call_direction is null or call_direction = '1')) or trans_channel_level_1 in ('Retail', 'IVR', 'eChat') ) and franchise = 'R' or franchise = 'C' group by to_date(concat(year,'-',month,'-',day)), a.ecid, (case when (trans_channel_level_1 = 'Call Center' and (call_direction is null or call_direction = '1')) then 'Call' when trans_channel_level_1 = 'Retail' then 'Retail' when trans_channel_level_1 = 'IVR' then 'IVR' when trans_channel_level_1 = 'eChat' then 'Chat' end)"""

print("CJA ---------------------->",query_cja)
df4_3_cja=spark.sql(query_cja)
display(df4_3_cja)


# COMMAND ----------

test=spark.sql("select a.ecid from app_customer_journey.vw_cbu_cust_journey_fct a inner join (select distinct base.ecid from mlmodel_data.DAT1_123_yuncheng_r_customer_base base) c on a.ecid=c.ecid")
display(test)

# COMMAND ----------

#/*Combine CJA and ignite txns*/ 41

df4_merged=df4_3_cja.select('trans_date', 'ecid', 'total_offline_events', 'channel', 'channel_type', 'offline_learn','offline_buy','offline_get','offline_use','offline_pay','offline_support' )\
  .unionAll(df4_2_ignite.select('trans_date', 'ecid', 'total_offline_events','CHANNEL', lit('ignite').alias('channel_type'), 'offline_learn','offline_buy','offline_get','offline_use','offline_pay','offline_support'))

df4_merged.createOrReplaceTempView("df4_merged")
print("Merge completed")


# COMMAND ----------

display(df4_merged)

# COMMAND ----------

#/*Add sales flag to offline data*/
#only consider within 30 days of offline events 
schema='mlmodel_data'
model_no= '123_yuncheng'
query_sales_flg="""select trans_date, a.ecid, a.channel, a.channel_type,
        avg(total_offline_events) as total_offline_events,
        avg(offline_learn) as offline_learn,
        avg(offline_buy) as offline_buy,
        avg(offline_get) as offline_get,
        avg(offline_use) as offline_use,
        avg(offline_pay) as offline_pay,
        avg(offline_support) as offline_support,
        sum(case when trans_date between date_sub(sale.date,30) and sale.date then sales_vol end) as sales_vol
from df4_merged a
left join (select date, ecid, sum(sales_vol) as sales_vol from """+schema+""".DAT2_"""+model_no+"""_r_sls_summary
            group by date, ecid) sale on a.ecid = sale.ecid
group by trans_date,  a.ecid,  a.channel, a.channel_type"""

print("Writing data",query_sales_flg)
df4_final=spark.sql(query_sales_flg)
display(df4_final)




# COMMAND ----------

display(df4_final.select('sales_vol').groupBy('sales_vol').count().orderBy('sales_vol'))

# COMMAND ----------

print("Writing into final table for this module")
df4_final.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT4_"+model_no+"_r_offline_data")
print("table "+schema+".DAT4_"+model_no+"_r_offline_data generated successfully")

# COMMAND ----------


