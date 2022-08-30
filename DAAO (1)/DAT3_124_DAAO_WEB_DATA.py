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

schema='mlmodel'
query_df7="""select * from """+schema+""".DAT3_126_web_common where hash_ban is not null and date_time >= to_date('"""+twoYearStart+"""') and  date_time< to_date('"""+twoYearEnd+"""')"""
print("Executing web data from wireless segment ---------------------->",query_df7)
df7=spark.sql(query_df7)
display(df7)

# COMMAND ----------

schema='mlmodel_data'
model_no= 'test_sg_f'
 
query_hash="""select distinct ecid,hash_account as hash_ban from hash_lookup.ecid_ban_can"""
print("Executing hash lookup ---------------------->",query_hash)
df3_hash_lookup=spark.sql(query_hash)

query_base="""select ecid from """+schema+""".DAT1_"""+model_no+"""_f_customer_base"""
print("Executing customer base ---------------------->",query_base)
df3_customer_base=spark.sql(query_base)
display(df3_customer_base)

# COMMAND ----------

display(df3_hash_lookup)

# COMMAND ----------

#problem in joining df7.hash_ban with df3_hash_lookup ban
df8=df7.join(df3_hash_lookup.alias('b'),df7.HASH_BAN == col('b.hash_ban'),how='inner')\
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

query_sales_flg="""select a.date_time, a.ecid,
        avg(total_web_visits) as total_web_visits,
        avg(web_Learn) as web_Learn,
        avg(web_Buy) as web_Buy,
        avg(web_Get) as web_Get,
        avg(web_Use) as web_Use,
        avg(web_Pay) as web_Pay,
        avg(web_Support) as web_Support,
        sum(case when a.date_time between date_sub(sale.date,30) and sale.date then sales_vol end) as sales_vol from df8 a left join (
        select date, ecid, sum(sales_vol) as sales_vol from """+schema+""".DAT2_"""+model_no+"""_f_sls_summary
        group by date, ecid) sale on a.ecid = sale.ecid
        group by a.date_time, a.ecid"""

print("Writing data",query_sales_flg)
df9=spark.sql(query_sales_flg)
display(df9)
df9.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT3_"+model_no+"_f_web_sls")
print("table "+schema+".DAT3_"+model_no+"_f_web_sls generated successfully")

print("#/* APP data : use table from wireless */ ###")


# COMMAND ----------

display(df9.select('sales_vol').groupBy('sales_vol').count().orderBy('sales_vol'))

# COMMAND ----------

query_df10="""select * from """+schema+""".DAT3_126_app_grpd_daily where where hash_ban is not null and date_time >= to_date('"""+twoYearStart+"""') and  date_time<= to_date('"""+twoYearEnd+"""')"""
print("Executing App data from wireless segment ---------------------->:",query_df10)
df10=spark.sql(query_df10)

print("#/*App data at ECID level*/")

df10=spark.sql(query_df10)

#/*App data at ECID level*/


df11=df10.join(df3_hash_lookup.alias('b'),df10.hash_ban == col('b.hash_ban'),how='inner')\
         .join(df3_customer_base.alias('base'),col('b.ecid') == col('base.ecid'),how='inner')\
         .groupby([df10.date_time,'b.ecid'])\
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


df11.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT3_"+model_no+"_f_app")
print("table "+schema+".DAT3_"+model_no+"_f_app generated successfully")


# COMMAND ----------

display(df11)

# COMMAND ----------


