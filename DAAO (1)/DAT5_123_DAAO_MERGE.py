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
from functools import reduce
from pyspark.sql import DataFrame


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

#import datetime
#today = datetime.datetime(2020, 7, 21)
#offset = ((today.weekday() - 6) % 7)
#twoYearEnd = ((today - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")
#twoYearStart_tmp=((today+ dateutil.relativedelta.relativedelta(weeks=-104)))
#offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)
#twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")
#OpenYrMth = today.strftime("%y"+"%m"+"%d")
#sixMoStart_tmp = today+ dateutil.relativedelta.relativedelta(months=-6)
#offset1 = ((sixMoStart_tmp.weekday() - 6) % 7)
#sixMoStart = (sixMoStart_tmp - timedelta(days=offset1)).strftime("%Y-%m-%d")
#print("OpenYrMth  is ", OpenYrMth)
#print("twoYearStart  is ", twoYearStart)
#print("twoYearEnd  is ", twoYearEnd)
#print("Start Rogers Customer base")
#print("six months ago  is ",sixMoStart)

# COMMAND ----------

print("Reading souce tables")
schema='mlmodel_data'
model_no= '123_yuncheng'
df1=spark.sql("""select * from """+schema+""".DAT1_"""+model_no+"""_r_customer_base""")
df2=spark.sql("""select * from """+schema+""".DAT3_"""+model_no+"""_r_web_sls""")
df3=spark.sql("""select * from """+schema+""".DAT3_"""+model_no+"""_r_app""")
df4=spark.sql("""select * from """+schema+""".DAT4_"""+model_no+"""_r_offline_data""")
df5=spark.sql("""select * from """+schema+""".DAT2_"""+model_no+"""_r_sls_pvt""")


# COMMAND ----------

display(df5)

# COMMAND ----------

#41 what is the difference between learn/buy and research? 'learn/buy' means no sell? if yes, why aggregated it in "purchase"
df5_web=df2.groupby(col('ecid'))\
  .agg(F.sum(when(col('total_web_visits') > 0 , 1)).alias('total_web_visits'),\
  F.sum(when((col('web_Learn') > 0) & (col('sales_vol').isNull()),1)).alias('web_Learn'),\
  F.sum(when((col('web_Learn') > 0) & (col('sales_vol')>0), 1)).alias('web_Learn_Research'),\
  F.sum(when((col('web_Buy') > 0) & (col('sales_vol').isNull()),1)).alias('web_Buy'),\
  F.sum(when((col('web_Buy') > 0) & (col('sales_vol')>0), 1)).alias('web_Buy_Research'),\
  F.sum(when(col('web_Get') > 0 , 1)).alias('web_Get'),\
  F.sum(when(col('web_Use') > 0 , 1)).alias('web_Use'),\
  F.sum(when(col('web_Pay') > 0 , 1)).alias('web_Pay'),\
  F.sum(when(col('web_Support') > 0 , 1)).alias('web_Support'),\
  F.sum(when((col('web_Use') > 0)  & (col('date_time') >= sixMoStart), 1)).alias('web_Use_6mo'),\
  F.sum(when((col('web_Pay') > 0 ) & (col('date_time') >= sixMoStart), 1)).alias('web_Pay_6mo'))


df5_app=df3.groupby(col('ecid'))\
  .agg(F.sum(when(col('total_web_visits') > 0 , 1)).alias('total_app_visits'),\
  F.sum(when((col('web_Learn') > 0),1)).alias('app_Learn'),\
  F.sum(when((col('web_Buy') > 0),1)).alias('app_Buy'),\
  F.sum(when(col('web_Get') > 0 , 1)).alias('app_Get'),\
  F.sum(when(col('web_Use') > 0 , 1)).alias('app_Use'),\
  F.sum(when(col('web_Pay') > 0 , 1)).alias('app_Pay'),\
  F.sum(when(col('web_Support') > 0 , 1)).alias('app_Support'),\
  F.sum(when((col('web_Use') > 0)  & (col('date_time') >= sixMoStart), 1)).alias('app_Use_6mo'),\
  F.sum(when((col('web_Pay') > 0 ) & (col('date_time') >= sixMoStart), 1)).alias('app_Pay_6mo'))	


df6_offline_call=df4.filter(col('channel').isin('Call','Retail','IVR','Chat'))\
  .groupby('ecid','channel')\
  .agg(F.sum(when(col('total_offline_events') > 0 , 1)).alias('total_offline_events'),\
  F.sum(when((col('offline_learn') > 0) & (col('sales_vol').isNull()) , 1)).alias('offline_Learn'),\
  F.sum(when((col('offline_learn') > 0) & (col('sales_vol')>0) , 1)).alias('offline_Learn_Research'),\
  F.sum(when((col('offline_buy') > 0) & (col('sales_vol').isNull()), 1)).alias('offline_Buy'),\
  F.sum(when((col('offline_buy') > 0 )& (col('sales_vol')>0), 1)).alias('offline_Buy_Research'),\
  F.sum(when(col('offline_get') > 0 , 1)).alias('offline_Get'),\
  F.sum(when(col('offline_use') > 0 , 1)).alias('offline_Use'),\
  F.sum(when(col('offline_pay') > 0 , 1)).alias('offline_Pay'),\
  F.sum(when(col('offline_support') > 0 , 1)).alias('offline_Support'),\
  F.sum(when((col('offline_use') > 0)  & (col('trans_date') >= sixMoStart), 1)).alias('offline_Use_6mo'),\
  F.sum(when((col('offline_pay') > 0 ) & (col('trans_date') >= sixMoStart), 1)).alias('offline_Pay_6mo'))

                 
#merged all data together, base table as "base for join"
df5_merged=df1.alias('base').join(df5_web.alias('web'),col('base.ecid') == col('web.ecid'),how='left')\
               .join(df5_app.alias('app'),col('base.ecid') == col('app.ecid'),how='left')\
               .join(df6_offline_call.filter(col('channel')=='Call').alias('call'),col('base.ecid') == col('call.ecid'),how='left')\
               .join(df6_offline_call.filter(col('channel')=='Retail').alias('retail'),col('base.ecid') == col('retail.ecid'),how='left')\
               .join(df6_offline_call.filter(col('channel')=='IVR').alias('ivr'),col('base.ecid') == col('ivr.ecid'),how='left')\
               .join(df6_offline_call.filter(col('channel')=='Chat').alias('chat'),col('base.ecid') == col('chat.ecid'),how='left')\
               .join(df5.alias('sales'),col('base.ecid') == col('sales.ecid'),how='left')\
               .selectExpr('base.ecid',\
#                             'base.type',\    #41 is that because ignite and legacy merges in the same table now
                            'cast(base.tenure_month as int) as tenure_month',\
                            'web.total_web_visits',\
                            ' web.web_Learn',\
                            ' web.web_Learn_Research',\
                            ' web.web_Buy',\
                            ' web.web_Buy_Research',\
                            ' web.web_Get',\
                            ' web.web_Use',\
                            ' web.web_Pay',\
                            ' web.web_Support',\
                            ' web_Use_6mo',\
                            ' web_Pay_6mo',\
                            'app.total_app_visits',\
                            ' app.app_Learn',\
                            ' app.app_Buy',\
                            ' app.app_Get',\
                            ' app.app_Use',\
                            ' app.app_Pay',\
                            ' app.app_Support',\
                            ' app_Use_6mo',\
                            ' app_Pay_6mo',\
                            'call.total_offline_events as total_call_events',\
                            ' call.offline_learn as call_learn',\
                            ' call.offline_learn_Research as call_learn_Research',\
                            ' call.offline_buy as call_buy',\
                            ' call.offline_buy_Research as call_buy_Research',\
                            ' call.offline_get as call_get',\
                            ' call.offline_use as call_use',\
                            ' call.offline_pay as call_pay',\
                            ' call.offline_support as call_support',\
                            ' call.offline_Use_6mo as call_use_6mo',\
                            ' call.offline_Pay_6mo as call_pay_6mo',\
                            'retail.total_offline_events as total_retail_events',\
                            ' retail.offline_learn as retail_learn',\
                            ' retail.offline_learn_Research as retail_learn_Research',\
                            ' retail.offline_buy as retail_buy',\
                            ' retail.offline_buy_Research as retail_buy_Research',\
                            ' retail.offline_get as retail_get',\
                            ' retail.offline_use as retail_use',\
                            ' retail.offline_pay as retail_pay',\
                            ' retail.offline_support as retail_support',\
                            ' retail.offline_Use_6mo as retail_use_6mo',\
                            ' retail.offline_Pay_6mo as retail_pay_6mo',\
                            'ivr.total_offline_events as total_ivr_events',\
                            ' ivr.offline_learn as ivr_learn',\
                            ' ivr.offline_learn_Research as ivr_learn_Research',\
                            ' ivr.offline_buy as ivr_buy',\
                            ' ivr.offline_buy_Research as ivr_buy_Research',\
                            ' ivr.offline_get as ivr_get',\
                            ' ivr.offline_use as ivr_use',\
                            ' ivr.offline_pay as ivr_pay',\
                            ' ivr.offline_support as ivr_support',\
                            ' ivr.offline_Use_6mo as ivr_use_6mo',\
                            ' ivr.offline_Pay_6mo as ivr_pay_6mo',\
                            'chat.total_offline_events as total_chat_events',\
                            ' chat.offline_learn as chat_learn',\
                            ' chat.offline_learn_Research as chat_learn_Research',\
                            ' chat.offline_buy as chat_buy',\
                            ' chat.offline_buy_Research as chat_buy_Research',\
                            ' chat.offline_get as chat_get',\
                            ' chat.offline_use as chat_use',\
                            ' chat.offline_pay as chat_pay',\
                            ' chat.offline_support as chat_support',\
                            ' chat.offline_Use_6mo as chat_use_6mo',\
                            ' chat.offline_Pay_6mo as chat_pay_6mo',\
                            'sales.sales_vol',\
                            ' sales.vol_digital as sales_digital',\
                            ' sales.vol_care as sales_care',\
                            ' sales.vol_retail as sales_retail',\
                            'sales.vol_other as sales_other')

  #/*Calculate channel preference*/
#as the name suggest, it focus on the proportion of the "web" component
#41 the "purchase" logic ?
df_merged_web=df5_merged.withColumn("channel",F.lit('web'))\
         .withColumn("learn_browse",(coalesce('web_Learn',F.lit(0))/(coalesce('web_Learn',F.lit(0)) + coalesce( 'app_Learn',F.lit(0)) + coalesce('call_learn' ,F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce( 'chat_learn',F.lit(0)))))\
         .withColumn("learn_research",(coalesce('web_Learn_Research',F.lit(0))/(coalesce('web_Learn_Research' ,F.lit(0)) + coalesce( 'call_learn_Research' ,F.lit(0)) + coalesce( 'retail_learn_Research',F.lit(0)) + coalesce( 'ivr_learn_Research',F.lit(0)) + coalesce( 'chat_learn_Research',F.lit(0)))))\
         .withColumn("buy_browse",(coalesce('web_Buy',F.lit(0))/(coalesce('web_Buy' ,F.lit(0)) + coalesce( 'app_Buy' ,F.lit(0)) + coalesce( 'call_buy' ,F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce( 'chat_buy',F.lit(0)))))\
         .withColumn("buy_research",(coalesce('web_Buy_Research',F.lit(0))/(coalesce('web_Buy_Research' ,F.lit(0)) + coalesce( 'call_buy_Research' ,F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce( 'chat_buy_Research',F.lit(0)))))\
         .withColumn("buy_sale",(coalesce('sales_digital',F.lit(0))/(coalesce('sales_vol',F.lit(0)))))\
         .withColumn("get",(coalesce('web_Get',F.lit(0))/(coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)) + coalesce('call_get',F.lit(0)) + coalesce('retail_get',F.lit(0)) + coalesce('ivr_get',F.lit(0)) + coalesce('chat_get',F.lit(0)))))\
         .withColumn("use",(coalesce('web_Use',F.lit(0))/(coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)) + coalesce( 'call_use' ,F.lit(0)) + coalesce('retail_use',F.lit(0)) + coalesce('ivr_use',F.lit(0)) + coalesce( 'chat_use',F.lit(0)))))\
         .withColumn("pay",(coalesce('web_Pay',F.lit(0))/(coalesce('web_Pay' ,F.lit(0)) + coalesce('app_Pay' ,F.lit(0)) + coalesce( 'call_pay' ,F.lit(0)) + coalesce( 'retail_pay',F.lit(0)) + coalesce('ivr_pay',F.lit(0)) + coalesce( 'chat_pay',F.lit(0)))))\
         .withColumn("support",(coalesce('web_Support',F.lit(0))/(coalesce('web_Support' ,F.lit(0)) + coalesce( 'app_Support' ,F.lit(0)) + coalesce( 'call_support' ,F.lit(0)) + coalesce( 'retail_support',F.lit(0)) + coalesce( 'ivr_support',F.lit(0)) + coalesce( 'chat_support',F.lit(0)))))\
         .withColumn("use_6mo",(coalesce('web_use_6mo',F.lit(0))/(coalesce('web_Use_6mo' ,F.lit(0)) + coalesce( 'app_Use_6mo' ,F.lit(0)) + coalesce( 'call_use_6mo' ,F.lit(0)) + coalesce( 'retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce( 'chat_use_6mo',F.lit(0)))))\
         .withColumn("pay_6mo",(coalesce('web_Pay_6mo',F.lit(0))/(coalesce('web_Pay_6mo' ,F.lit(0)) + coalesce('app_Pay_6mo' ,F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce( 'chat_pay_6mo',F.lit(0)))))\
         .withColumn("purchase",( (coalesce('web_Learn',F.lit(0)) + coalesce('web_Learn_Research',F.lit(0)) + coalesce('web_Buy',F.lit(0)) + coalesce('web_Buy_Research',F.lit(0)) + coalesce('sales_digital',F.lit(0)))/( (coalesce('web_Learn' ,F.lit(0)) + coalesce('app_Learn' ,F.lit(0)) + coalesce( 'call_learn' ,F.lit(0)) + coalesce( 'retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce( 'chat_learn',F.lit(0)))  +(coalesce('web_Learn_Research' ,F.lit(0)) + coalesce( 'call_learn_Research' ,F.lit(0)) + coalesce( 'retail_learn_Research',F.lit(0)) + coalesce( 'ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0)))  +(coalesce('web_Buy' ,F.lit(0)) + coalesce( 'app_Buy' ,F.lit(0)) + coalesce('call_buy' ,F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0))) +(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce( 'retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0))) +(coalesce('sales_vol',F.lit(0))))))\
         .withColumn("service",((coalesce('web_use_6mo',F.lit(0)) + coalesce('web_Pay_6mo',F.lit(0)) + coalesce('web_Support',F.lit(0))) / ((coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo' ,F.lit(0)) + coalesce( 'call_use_6mo' ,F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))+(coalesce('web_Pay_6mo' ,F.lit(0)) + coalesce( 'app_Pay_6mo' ,F.lit(0)) + coalesce( 'call_pay_6mo',F.lit(0)) + coalesce( 'retail_pay_6mo',F.lit(0)) + coalesce( 'ivr_pay_6mo',F.lit(0)) + coalesce( 'chat_pay_6mo',F.lit(0))) +(coalesce('web_Support' ,F.lit(0)) + coalesce( 'app_Support' ,F.lit(0)) + coalesce( 'call_support' ,F.lit(0)) + coalesce( 'retail_support',F.lit(0)) + coalesce( 'ivr_support',F.lit(0)) + coalesce( 'chat_support',F.lit(0))))))\
         .selectExpr('ecid',\
                     'channel',\
                     'learn_browse',\
                     'learn_research',\
                     'buy_browse',\
                     'buy_research',\
                     'buy_sale',\
                     'get',\
                     'use',\
                     'pay',\
                     'support',\
                     'use_6mo',\
                     'pay_6mo',\
                     'purchase',\
                     'service'
                     )

#it focus on the proportion of the "app" component
#41 why 'research' for "app" here has different logic whith 'research in web', same question with purchase part, different numerator logic
df_merged_app=df5_merged.withColumn("channel",F.lit('app'))\
         .withColumn("learn_browse",(coalesce('app_Learn',F.lit(0))/(coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0)))))\
         .withColumn("learn_research",F.lit(0))\
         .withColumn("buy_browse",(coalesce('app_Buy',F.lit(0))/(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0)))))\
         .withColumn("buy_research",F.lit(0))\
         .withColumn("buy_sale",F.lit(0))\
         .withColumn("get",(coalesce('app_Get',F.lit(0))/(coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)) + coalesce('call_get',F.lit(0)) + coalesce('retail_get',F.lit(0)) + coalesce('ivr_get',F.lit(0)) + coalesce('chat_get',F.lit(0)))))\
         .withColumn("use",(coalesce('app_Use',F.lit(0))/(coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)) + coalesce('call_use',F.lit(0)) + coalesce('retail_use',F.lit(0)) + coalesce('ivr_use',F.lit(0)) + coalesce('chat_use',F.lit(0)))))\
         .withColumn("pay",(coalesce('app_Pay',F.lit(0))/(coalesce('web_Pay',F.lit(0)) + coalesce('app_Pay',F.lit(0)) + coalesce('call_pay',F.lit(0)) + coalesce('retail_pay',F.lit(0)) + coalesce('ivr_pay',F.lit(0)) + coalesce('chat_pay',F.lit(0)))))\
         .withColumn("support",(coalesce('app_Support',F.lit(0))/(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0)))))\
         .withColumn("use_6mo",(coalesce('app_use_6mo',F.lit(0))/(coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))))\
         .withColumn("pay_6mo",(coalesce('app_Pay_6mo',F.lit(0))/(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0)))))\
         .withColumn("purchase",( (coalesce('app_Learn',F.lit(0))  + coalesce('app_Buy',F.lit(0)) ) / ( (coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0))) +(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0))) +(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0))) +(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0))) +(coalesce('sales_vol',F.lit(0))))))\
         .withColumn("service",( (coalesce('app_use_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('app_Support',F.lit(0))) / ( (coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0))) +(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0))) +(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0))))))\
         .selectExpr('ecid',\
                     'channel',\
                     'learn_browse',\
                     'learn_research',\
                     'buy_browse',\
                     'buy_research',\
                     'buy_sale',\
                     'get',\
                     'use',\
                     'pay',\
                     'support',\
                     'use_6mo',\
                     'pay_6mo',\
                     'purchase',\
                     'service')


          #Added

df_merged_call=df5_merged.withColumn("channel",F.lit('call'))\
         .withColumn("learn_browse",(coalesce('call_learn',F.lit(0))/(coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0)))))\
         .withColumn("learn_research",(coalesce('call_learn_Research',F.lit(0))/(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0)))))\
         .withColumn("buy_browse",(coalesce('call_buy',F.lit(0))/(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0)))))\
         .withColumn("buy_research",(coalesce('call_buy_Research',F.lit(0))/(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0)))))\
         .withColumn("buy_sale",(coalesce('sales_care',F.lit(0))/(coalesce('sales_vol',F.lit(0)))))\
         .withColumn("get",(coalesce('call_get',F.lit(0))/(coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)) + coalesce('call_get',F.lit(0)) + coalesce('retail_get',F.lit(0)) + coalesce('ivr_get',F.lit(0)) + coalesce('chat_get',F.lit(0)))))\
         .withColumn("use",(coalesce('call_use',F.lit(0))/(coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)) + coalesce('call_use',F.lit(0)) + coalesce('retail_use',F.lit(0)) + coalesce('ivr_use',F.lit(0)) + coalesce('chat_use',F.lit(0)))))\
         .withColumn("pay",(coalesce('call_pay',F.lit(0))/(coalesce('web_Pay',F.lit(0)) + coalesce('app_Pay',F.lit(0)) + coalesce('call_pay',F.lit(0)) + coalesce('retail_pay',F.lit(0)) + coalesce('ivr_pay',F.lit(0)) + coalesce('chat_pay',F.lit(0)))))\
         .withColumn("support",(coalesce('call_support',F.lit(0))/(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0)))))\
         .withColumn("use_6mo",(coalesce('call_use_6mo',F.lit(0))/(coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))))\
         .withColumn("pay_6mo",(coalesce('call_pay_6mo',F.lit(0))/(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0)))))\
         .withColumn("purchase",( (coalesce('call_Learn',F.lit(0)) + coalesce('call_Learn_Research',F.lit(0)) + coalesce('call_Buy',F.lit(0)) + coalesce('call_Buy_Research',F.lit(0)) + coalesce('sales_care',F.lit(0))) / ( (coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0))) +(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0)))  +(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0))) +(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0))) +(coalesce('sales_vol',F.lit(0))))))\
         .withColumn("service",( (coalesce('call_use_6mo',F.lit(0)) + coalesce('call_Pay_6mo',F.lit(0)) + coalesce('call_Support',F.lit(0))) / ( (coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))  +(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0))) +(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0))))))\
         .selectExpr('ecid',\
                     'channel',\
                     'learn_browse',\
                     'learn_research',\
                     'buy_browse',\
                     'buy_research',\
                     'buy_sale',\
                     'get',\
                     'use',\
                     'pay',\
                     'support',\
                     'use_6mo',\
                     'pay_6mo',\
                     'purchase',\
                     'service')


df_merged_retail=df5_merged.withColumn("channel",F.lit('retail'))\
         .withColumn("learn_browse",(coalesce('retail_learn',F.lit(0))/(coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0)))))\
         .withColumn("learn_research",(coalesce('retail_learn_Research',F.lit(0))/(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0)))))\
         .withColumn("buy_browse",(coalesce('retail_buy',F.lit(0))/(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0)))))\
         .withColumn("buy_research",(coalesce('retail_buy_Research',F.lit(0))/(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0)))))\
         .withColumn("buy_sale",(coalesce('sales_retail',F.lit(0))/(coalesce('sales_vol',F.lit(0)))))\
         .withColumn("get",(coalesce('retail_get',F.lit(0))/(coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)) + coalesce('call_get',F.lit(0)) + coalesce('retail_get',F.lit(0)) + coalesce('ivr_get',F.lit(0)) + coalesce('chat_get',F.lit(0)))))\
         .withColumn("use",(coalesce('retail_use',F.lit(0))/(coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)) + coalesce('call_use',F.lit(0)) + coalesce('retail_use',F.lit(0)) + coalesce('ivr_use',F.lit(0)) + coalesce('chat_use',F.lit(0)))))\
         .withColumn("pay",(coalesce('retail_pay',F.lit(0))/(coalesce('web_Pay',F.lit(0)) + coalesce('app_Pay',F.lit(0)) + coalesce('call_pay',F.lit(0)) + coalesce('retail_pay',F.lit(0)) + coalesce('ivr_pay',F.lit(0)) + coalesce('chat_pay',F.lit(0)))))\
         .withColumn("support",(coalesce('retail_support',F.lit(0))/(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0)))))\
         .withColumn("use_6mo",(coalesce('retail_use_6mo',F.lit(0))/(coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))))\
         .withColumn("pay_6mo",(coalesce('retail_pay_6mo',F.lit(0))/(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0)))))\
         .withColumn("purchase",( (coalesce('retail_Learn',F.lit(0)) + coalesce('retail_Learn_Research',F.lit(0)) + coalesce('retail_Buy',F.lit(0)) + coalesce('retail_Buy_Research',F.lit(0)) + coalesce('sales_retail',F.lit(0))) / ( (coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0)))  +(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0))) +(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0))) +(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0))) +(coalesce('sales_vol',F.lit(0))) )))\
         .withColumn("service",( (coalesce('retail_use_6mo',F.lit(0)) + coalesce('retail_Pay_6mo',F.lit(0)) + coalesce('retail_Support',F.lit(0))) / ( (coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0))) +(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0))) +(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0))))))\
         .selectExpr('ecid',\
                     'channel',\
                     'learn_browse',\
                     'learn_research',\
                     'buy_browse',\
                     'buy_research',\
                     'buy_sale',\
                     'get',\
                     'use',\
                     'pay',\
                     'support',\
                     'use_6mo',\
                     'pay_6mo',\
                     'purchase',\
                     'service')

df_merged_ivr=df5_merged.withColumn("channel",F.lit('ivr'))\
         .withColumn("learn_browse",(coalesce('ivr_learn',F.lit(0))/(coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0)))))\
         .withColumn("learn_research",(coalesce('ivr_learn_Research',F.lit(0))/(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0)))))\
         .withColumn("buy_browse",(coalesce('ivr_buy',F.lit(0))/(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0)))))\
         .withColumn("buy_research",(coalesce('ivr_buy_Research',F.lit(0))/(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0)))))\
         .withColumn("buy_sale",F.lit(0))\
         .withColumn("get",(coalesce('ivr_get',F.lit(0))/(coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)) + coalesce('call_get',F.lit(0)) + coalesce('retail_get',F.lit(0)) + coalesce('ivr_get',F.lit(0)) + coalesce('chat_get',F.lit(0)))))\
         .withColumn("use",(coalesce('ivr_use',F.lit(0))/(coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)) + coalesce('call_use',F.lit(0)) + coalesce('retail_use',F.lit(0)) + coalesce('ivr_use',F.lit(0)) + coalesce('chat_use',F.lit(0)))))\
         .withColumn("pay",(coalesce('ivr_pay',F.lit(0))/(coalesce('web_Pay',F.lit(0)) + coalesce('app_Pay',F.lit(0)) + coalesce('call_pay',F.lit(0)) + coalesce('retail_pay',F.lit(0)) + coalesce('ivr_pay',F.lit(0)) + coalesce('chat_pay',F.lit(0)))))\
         .withColumn("support",(coalesce('ivr_support',F.lit(0))/(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0)))))\
         .withColumn("use_6mo",(coalesce('ivr_use_6mo',F.lit(0))/(coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))))\
         .withColumn("pay_6mo",(coalesce('ivr_pay_6mo',F.lit(0))/(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0)))))\
         .withColumn("purchase",( (coalesce('ivr_Learn',F.lit(0)) + coalesce('ivr_Learn_Research',F.lit(0)) + coalesce('ivr_Buy',F.lit(0)) + coalesce('ivr_Buy_Research',F.lit(0))) / ( (coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0))) +(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0))) +(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0))) +(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0))) +(coalesce('sales_vol',F.lit(0))))))\
         .withColumn("service",( (coalesce('ivr_use_6mo',F.lit(0)) + coalesce('ivr_Pay_6mo',F.lit(0)) + coalesce('ivr_Support',F.lit(0))) / ( (coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0))) +(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0))) +(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0))) )))\
         .selectExpr('ecid',\
                     'channel',\
                     'learn_browse',\
                     'learn_research',\
                     'buy_browse',\
                     'buy_research',\
                     'buy_sale',\
                     'get',\
                     'use',\
                     'pay',\
                     'support',\
                     'use_6mo',\
                     'pay_6mo',\
                     'purchase',\
                     'service')

df_merged_chat=df5_merged.withColumn("channel",F.lit('chat'))\
         .withColumn("learn_browse",(coalesce('chat_learn',F.lit(0))/(coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0)))))\
         .withColumn("learn_research",(coalesce('chat_learn_Research',F.lit(0))/(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0)))))\
         .withColumn("buy_browse",(coalesce('chat_buy',F.lit(0))/(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0)))))\
         .withColumn("buy_research",(coalesce('chat_buy_Research',F.lit(0))/(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0)))))\
         .withColumn("buy_sale",F.lit(0))\
         .withColumn("get",(coalesce('chat_get',F.lit(0))/(coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)) + coalesce('call_get',F.lit(0)) + coalesce('retail_get',F.lit(0)) + coalesce('ivr_get',F.lit(0)) + coalesce('chat_get',F.lit(0)))))\
         .withColumn("use",(coalesce('chat_use',F.lit(0))/(coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)) + coalesce('call_use',F.lit(0)) + coalesce('retail_use',F.lit(0)) + coalesce('ivr_use',F.lit(0)) + coalesce('chat_use',F.lit(0)))))\
         .withColumn("pay",(coalesce('chat_pay',F.lit(0))/(coalesce('web_Pay',F.lit(0)) + coalesce('app_Pay',F.lit(0)) + coalesce('call_pay',F.lit(0)) + coalesce('retail_pay',F.lit(0)) + coalesce('ivr_pay',F.lit(0)) + coalesce('chat_pay',F.lit(0)))))\
         .withColumn("support",(coalesce('chat_support',F.lit(0))/(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0)))))\
         .withColumn("use_6mo",(coalesce('chat_use_6mo',F.lit(0))/(coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))))\
         .withColumn("pay_6mo",(coalesce('chat_pay_6mo',F.lit(0))/(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0)))))\
         .withColumn("purchase",( (coalesce('ivr_Learn',F.lit(0)) + coalesce('ivr_Learn_Research',F.lit(0)) + coalesce('ivr_Buy',F.lit(0)) + coalesce('ivr_Buy_Research',F.lit(0))) / ( (coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0))) +(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0))) +(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0))) +(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0))) +(coalesce('sales_vol',F.lit(0))))))\
         .withColumn("service",( (coalesce('chat_use_6mo',F.lit(0)) + coalesce('chat_Pay_6mo',F.lit(0)) + coalesce('chat_Support',F.lit(0))) / ( (coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))  +(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0))) +(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0))) ) ))\
         .selectExpr('ecid',\
                     'channel',\
                     'learn_browse',\
                     'learn_research',\
                     'buy_browse',\
                     'buy_research',\
                     'buy_sale',\
                     'get',\
                     'use',\
                     'pay',\
                     'support',\
                     'use_6mo',\
                     'pay_6mo',\
                     'purchase',\
                     'service')

df_merged_digital=df5_merged.withColumn("channel",F.lit('digital'))\
         .withColumn("learn_browse",((coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)))/(coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0)))))\
         .withColumn("learn_research",(coalesce('web_Learn_Research',F.lit(0))/(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0)))))\
         .withColumn("buy_browse",((coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)))/(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0)))))\
         .withColumn("buy_research",(coalesce('web_Buy_Research',F.lit(0))/(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0)))))\
         .withColumn("buy_sale",(coalesce('sales_digital',F.lit(0))/(coalesce('sales_vol',F.lit(0)))))\
         .withColumn("get",((coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)))/(coalesce('web_Get',F.lit(0)) + coalesce('app_Get',F.lit(0)) + coalesce('call_get',F.lit(0)) + coalesce('retail_get',F.lit(0)) + coalesce('ivr_get',F.lit(0)) + coalesce('chat_get',F.lit(0)))))\
         .withColumn("use",((coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)))/(coalesce('web_Use',F.lit(0)) + coalesce('app_Use',F.lit(0)) + coalesce('call_use',F.lit(0)) + coalesce('retail_use',F.lit(0)) + coalesce('ivr_use',F.lit(0)) + coalesce('chat_use',F.lit(0)))))\
         .withColumn("pay",((coalesce('web_Pay',F.lit(0)) + coalesce('app_Pay',F.lit(0)))/(coalesce('web_Pay',F.lit(0)) + coalesce('app_Pay',F.lit(0)) + coalesce('call_pay',F.lit(0)) + coalesce('retail_pay',F.lit(0)) + coalesce('ivr_pay',F.lit(0)) + coalesce('chat_pay',F.lit(0)))))\
         .withColumn("support",((coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)))/(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0)))))\
         .withColumn("use_6mo",((coalesce('web_use_6mo',F.lit(0))+coalesce('app_Use_6mo',F.lit(0)))/(coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))))\
         .withColumn("pay_6mo",((coalesce('web_Pay_6mo',F.lit(0))+coalesce('app_Pay_6mo',F.lit(0)))/(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0)))))\
         .withColumn("purchase",( (coalesce('web_Learn',F.lit(0)) + coalesce('web_Learn_Research',F.lit(0)) + coalesce('web_Buy',F.lit(0)) + coalesce('web_Buy_Research',F.lit(0)) + coalesce('sales_digital',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('app_Buy',F.lit(0)) )  / ( (coalesce('web_Learn',F.lit(0)) + coalesce('app_Learn',F.lit(0)) + coalesce('call_learn',F.lit(0)) + coalesce('retail_learn',F.lit(0)) + coalesce('ivr_learn',F.lit(0)) + coalesce('chat_learn',F.lit(0))) +(coalesce('web_Learn_Research',F.lit(0)) + coalesce('call_learn_Research',F.lit(0)) + coalesce('retail_learn_Research',F.lit(0)) + coalesce('ivr_learn_Research',F.lit(0)) + coalesce('chat_learn_Research',F.lit(0))) +(coalesce('web_Buy',F.lit(0)) + coalesce('app_Buy',F.lit(0)) + coalesce('call_buy',F.lit(0)) + coalesce('retail_buy',F.lit(0)) + coalesce('ivr_buy',F.lit(0)) + coalesce('chat_buy',F.lit(0))) +(coalesce('web_Buy_Research',F.lit(0)) + coalesce('call_buy_Research',F.lit(0)) + coalesce('retail_buy_Research',F.lit(0)) + coalesce('ivr_buy_Research',F.lit(0)) + coalesce('chat_buy_Research',F.lit(0))) +(coalesce('sales_vol',F.lit(0))) )))\
         .withColumn("service",( (coalesce('web_use_6mo',F.lit(0)) + coalesce('web_Pay_6mo',F.lit(0)) + coalesce('web_Support',F.lit(0))+ coalesce('app_Use_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('app_Support',F.lit(0))) / ( (coalesce('web_Use_6mo',F.lit(0)) + coalesce('app_Use_6mo',F.lit(0)) + coalesce('call_use_6mo',F.lit(0)) + coalesce('retail_use_6mo',F.lit(0)) + coalesce('ivr_use_6mo',F.lit(0)) + coalesce('chat_use_6mo',F.lit(0)))  +(coalesce('web_Pay_6mo',F.lit(0)) + coalesce('app_Pay_6mo',F.lit(0)) + coalesce('call_pay_6mo',F.lit(0)) + coalesce('retail_pay_6mo',F.lit(0)) + coalesce('ivr_pay_6mo',F.lit(0)) + coalesce('chat_pay_6mo',F.lit(0)))  +(coalesce('web_Support',F.lit(0)) + coalesce('app_Support',F.lit(0)) + coalesce('call_support',F.lit(0)) + coalesce('retail_support',F.lit(0)) + coalesce('ivr_support',F.lit(0)) + coalesce('chat_support',F.lit(0))))))\
         .selectExpr('ecid',\
                     'channel',\
                     'learn_browse',\
                     'learn_research',\
                     'buy_browse',\
                     'buy_research',\
                     'buy_sale',\
                     'get',\
                     'use',\
                     'pay',\
                     'support',\
                     'use_6mo',\
                     'pay_6mo',\
                     'purchase',\
                     'service')


# COMMAND ----------

display(df_merged_digital)
