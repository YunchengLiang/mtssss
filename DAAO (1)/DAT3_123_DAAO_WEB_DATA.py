# Databricks notebook source
import sys
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


'''offset = ((datetime.today().weekday() - 6) % 7)
twoYearEnd = ((datetime.today() - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")
twoYearStart_tmp=(( datetime.today()+ dateutil.relativedelta.relativedelta(weeks=-104)))
offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)
twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")
OpenYrMth = datetime.today().strftime("%y"+"%m"+"%d")
sixMoStart_tmp = datetime.today()+ dateutil.relativedelta.relativedelta(months=-6)#6 month before today
offset1 = ((sixMoStart_tmp.weekday() - 6) % 7)# weekday of '6 month before today'
sixMoStart = (sixMoStart_tmp - timedelta(days=offset1)).strftime("%Y-%m-%d") # last sunday before '6 month before today'
print("OpenYrMth  is ", OpenYrMth)
print("twoYearStart  is ", twoYearStart)
print("twoYearEnd  is ", twoYearEnd)
print("six months ago  is ",sixMoStart)
#41 question, what is the sixmonths-tmp used for ''' 



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

# /*********Rogers Wireless*************/
#/*Get web usage by LBGUPS
#DAT3_125_web_common by Anthoy, schema to mlmodel instead of mlmodel_data
schema='mlmodel_data'
model_no= '123_yuncheng'
query_df7="""select * from """+schema+""".DAT3_125_web_common where hash_ban is not null and date_time >= to_date('"""+twoYearStart+"""') and  date_time< to_date('"""+twoYearEnd+"""')"""
print("Executing web data from wireless segment ---------------------->",query_df7)
df7=spark.sql(query_df7)
display(df7)
print(df7.count()) #105504740->gone

# COMMAND ----------

#41 sha2(cast(cast(account as bigint) as string),256) used in the original code as hash_ban
query_hash="""select distinct ecid,hash_account as hash_ban from hash_lookup.ecid_ban_can"""
print("Executing hash lookup ---------------------->",query_hash)
df3_hash_lookup=spark.sql(query_hash)
display(df3_hash_lookup)


# COMMAND ----------

query_base="""select ecid from """+schema+""".DAT1_"""+model_no+"""_r_customer_base"""
print("Executing customer base ---------------------->",query_base)
df3_customer_base=spark.sql(query_base)
display(df3_customer_base)

# COMMAND ----------

#df3_hash_lookup used as a filter(hash_ban constaint) for df7(DAT3_125_web_common)
#df3_customer_base used as an another filter(ecid constraint) for df7(DAT3_125_web_common) and df3_hash_lookup
df8=df7.join(df3_hash_lookup.alias('b'),df7.hash_ban == col('b.hash_ban'),how='inner')\
       .join(df3_customer_base.alias('base'),col('b.ecid') == col('base.ecid'),how='inner')\
       .groupby("date_time","b.ecid")\
       .agg(F.sum('total_web_visits').alias('total_web_visits'),
            F.sum('web_Learn').alias('web_Learn'),
            F.sum('web_Buy').alias('web_Buy'),
            F.sum('web_Get').alias('web_Get'),
            F.sum('web_Use').alias('web_Use'),
            F.sum('web_Pay').alias('web_Pay'),
            F.sum('web_Support').alias('web_Support'))\
            .selectExpr('date_time',
                   'ecid',
                   'total_web_visits',
                   'web_Learn',
                   'web_Buy',
                   'web_Get',
                   'web_Use',
                   'web_Pay',
                   'web_Support')

df8.createOrReplaceTempView("df8")
print("Registered table df8")


# COMMAND ----------

display(df8)

# COMMAND ----------

schema='mlmodel_data'
model_no= '123_yuncheng'
test=spark.sql("select date, ecid, sum(sales_vol) as sales_vol from mlmodel_data.DAT2_123_yuncheng_r_sls_summary group by date, ecid")
display(test)

# COMMAND ----------

#/*Add sales to web data*/
#TODO: verify date logic from Jenni
#for case when, default else is null, only calculate sum for sales_vol occured after webvisit within 30 days
#so a.datetime supposed to have different logic with sale data's date 
query_sales_flg="""select a.date_time, a.ecid,
        avg(total_web_visits) as total_web_visits,
        avg(web_Learn) as web_Learn,
        avg(web_Buy) as web_Buy,
        avg(web_Get) as web_Get,
        avg(web_Use) as web_Use,
        avg(web_Pay) as web_Pay,
        avg(web_Support) as web_Support,
        sum(case when a.date_time between date_sub(sale.date, 30) and sale.date then sales_vol end) as sales_vol from df8 a left join (select date, ecid, sum(sales_vol) as sales_vol from """+schema+""".DAT2_"""+model_no+"""_r_sls_summary group by date, ecid) sale on a.ecid = sale.ecid group by a.date_time, a.ecid"""

print("Writing data",query_sales_flg)
df9=spark.sql(query_sales_flg)
display(df9)
df9.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT3_"+model_no+"_r_web_sls")
print("table "+schema+".DAT3_"+model_no+"_r_web_sls generated successfully")

print("Strating APP data")


# COMMAND ----------

display(df9.select('sales_vol').groupBy('sales_vol').count().orderBy('sales_vol'))

# COMMAND ----------

#DAT3_125_app_grpd_daily
query_df10="""select * from """+schema+""".DAT3_125_app_grpd_daily where hash_ban is not null and date_time >= to_date('"""+twoYearStart+"""') and  date_time< to_date('"""+twoYearEnd+"""')"""
print("Executing App data from wireless segment ---------------------->:", query_df10)
df10=spark.sql(query_df10)

#/*App data at ECID level*/
 
#df3_hash_lookup used as a filter(hash_ban constaint) for df10(DAT3_125_app_grpd_daily)
#df3_customer_base used as an another filter(ecid constraint) for df10(DAT3_125_app_grpd_daily) and df3_hash_lookup

df11=df10.join(df3_hash_lookup.alias('b'),df10.hash_ban == col('b.hash_ban'),how='inner')\
         .join(df3_customer_base.alias('base'),col('b.ecid') == col('base.ecid'),how='inner')\
       .groupby([df10.date_time,'base.ecid'])\
       .agg(F.sum('total_web_visits').alias('total_web_visits'),
            F.sum('web_Learn').alias('web_Learn'),
            F.sum('web_Buy').alias('web_Buy'),
            F.sum('web_Get').alias('web_Get'),
            F.sum('web_Use').alias('web_Use'),
            F.sum('web_Pay').alias('web_Pay'),
            F.sum('web_Support').alias('web_Support'))\
            .selectExpr('date_time',
                   'ecid',
                   'total_web_visits',
                   'web_Learn',
                   'web_Buy',
                   'web_Get',
                   'web_Use',
                   'web_Pay',
                   'web_Support')
display(df11)
df11.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT3_"+model_no+"_r_app")
print("table "+schema+".DAT3_"+model_no+"_r_app generated successfully")


# COMMAND ----------


