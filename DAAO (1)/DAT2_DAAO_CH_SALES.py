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
print("Start Rogers sales")

'''offset = ((datetime.today().weekday() - 6) % 7)
twoYearEnd = ((datetime.today() - timedelta(days=offset))+timedelta(days=-1)).strftime("%Y-%m-%d")
twoYearStart_tmp=(( datetime.today()+ dateutil.relativedelta.relativedelta(weeks=-104)))
offset2 = ((twoYearStart_tmp.weekday() - 6) % 7)
twoYearStart=(twoYearStart_tmp - timedelta(days=offset2)).strftime("%Y-%m-%d")
OpenYrMth = datetime.today().strftime("%y"+"%m"+"%d")
print("OpenYrMth  is ", OpenYrMth)
print("twoYearStart  is ", twoYearStart)
print("twoYearEnd  is ", twoYearEnd)

print("Start Rogers Sales")
'''


# COMMAND ----------

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

# MAGIC %md
# MAGIC #### Sales

# COMMAND ----------

################################### Sales ###########################################
'''
This query
'''
# app_ibro.ibro_subscriber_activity_hist
# it has function of self drop duplicates
query_sales="""select distinct  a.activity_date ,a.customer_account ,a.enterprise_id ,a.product_code ,a.customer_brand ,a.ACTIVITY ,a.ACTIVITY_DEALER ,a.CUSTOMER_MULTI_PRODUCT ,a.PRODUCT_LOB ,a.PRODUCT_PRICE_PLAN ,a.ACTIVITY_REASON ,a.NEW_SALE ,a.SYSTEM_TRANSFER_IN ,a.TRANSFER_IN ,a.INTERBRAND_IN ,a.SEASONAL_IN ,a.SYSTEM_ADDRESS_TRANSFER_IN ,a.INTERBRAND_ADDRESS_IN ,a.NET_ADDS ,a.GROSS_ADDS ,a.customer_segment ,a.opening ,a.closing ,a.ATTR_ADDRESS_CONTRACT_GROUP ,a.product_segment ,a.product_source ,a.product_quantity ,a.product_brand from app_ibro.ibro_subscriber_activity_hist a where a.activity_type = 'A' and a.ACTIVITY in ('NAC','STI') and a.product_lob_unit = 'Y' and a.product_segment = 'CBU' and a.activity_dealer <> '' and a.activity_date >= to_date('"""+twoYearStart+"""') and a.activity_date <= to_date('"""+twoYearEnd+"""') order by a.customer_brand ,a.activity_date ,a.enterprise_id ,a.customer_account ,a.PRODUCT_LOB ,a.ACTIVITY"""
print("Sales query 1 ", query_sales)
df2_sales=spark.sql(query_sales).cache()
display(df2_sales)

# COMMAND ----------

##Sales Channel
        
query_sales_channel="""select distinct cast(employee_id as string) as activity_dealer ,effective_date ,expire_date ,custom_channel_level_1 ,custom_channel_level_2 ,custom_channel_level_3 from app_sales_insight.employeehierarchyscd_ref where effective_date >= to_date('"""+twoYearStart+"""') and cast(employee_id as string) <> '' and custom_channel_level_1 <> '' union distinct  select distinct sgi_id as activity_dealer ,effective_date ,expire_date ,custom_channel_level_1 ,custom_channel_level_2 ,custom_channel_level_3 from app_sales_insight.employeehierarchyscd_ref where effective_date >=to_date('"""+twoYearStart+"""') and sgi_id <> '' and custom_channel_level_1 <> '' union distinct  select distinct commission_id as activity_dealer ,effective_date ,expire_date ,custom_channel_level_1 ,custom_channel_level_2 ,custom_channel_level_3 from app_sales_insight.employeehierarchyscd_ref where effective_date >=to_date('"""+twoYearStart+"""') and commission_id <> '' and custom_channel_level_1 <> ''"""


print("Sales channel query ", query_sales_channel)
df2_sales_channel=spark.sql(query_sales_channel).cache()
print("Count of df2_sales_channel:",df2_sales_channel.count())


# COMMAND ----------

print("Join channels to Sales")
        
df2_sales_join_channel=df2_sales.alias('a').join(df2_sales_channel.alias('b'),[col('a.activity_dealer') == col('b.activity_dealer'),col('a.activity_date') >= to_date(col('b.effective_date')),col('a.activity_date')<= to_date(col('b.expire_date'))],how='left').selectExpr("a.*","b.custom_channel_level_1","b.custom_channel_level_2","b.custom_channel_level_3")

display(df2_sales_join_channel)


# COMMAND ----------

schema= 'mlmodel_data'
model_no_r= '123_yuncheng'
query_customer_base="""select distinct ecid from """+schema+""".DAT1_"""+model_no_r+"""_r_customer_base"""
print("Customer base for Rogers ", query_customer_base)
df2_customer_base=spark.sql(query_customer_base)
display(df2_customer_base)

# COMMAND ----------

df2_customer_base.count()

# COMMAND ----------

#/*Sales clean up - channels and aggregate to ECID level for Rogers*/
#TODO: replace schema and model_no_r with the args
#join to only keep customers with ecid in rogers_customer_base
df2_sales_summary=df2_sales_join_channel.alias('a').join(df2_customer_base.alias('b'),col('a.enterprise_id') == col('b.ecid'),how='inner').withColumn("channel", when((upper(col("custom_channel_level_2")).isin(['CALL CENTRES','TELESALES','FIDO CALL CENTRES','ISR','UTESALES'])),"Call").when((upper(col("custom_channel_level_2")).isin(['DWBS', 'E-COMMERCE'])),"Digital").when ((upper(col("custom_channel_level_1")).like("%RETAIL")),"Retail").otherwise("other")).groupby("activity_date","enterprise_id","channel").agg(count(lit(1)).alias("sales_vol")).selectExpr("activity_date as date","enterprise_id as ecid","channel","sales_vol")

display(df2_sales_summary)
print("writing  sales data in schema " + schema)
df2_sales_summary.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT2_"+model_no_r+"_r_sls_summary")
print(schema+".DAT2_"+model_no_r+"_r_sls_summary generated successfully")
        


# COMMAND ----------

#/*Pivot sales data - to prepare for joining later*/
df2_r_sales_pvt=df2_sales_summary.groupby('ecid')\
.agg(F.sum('sales_vol').alias('sales_vol'),
    F.sum(when(col('channel') == 'Digital',col('sales_vol'))).alias('vol_digital'),
    F.sum(when(col('channel') == 'Call',col('sales_vol'))).alias('vol_care'),
    F.sum(when(col('channel') == 'Retail',col('sales_vol'))).alias('vol_retail'),
    F.sum(when(col('channel') == 'other',col('sales_vol'))).alias('vol_other'))

display(df2_r_sales_pvt)
print("writing pivotted sales data in schema " + schema)
df2_r_sales_pvt.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT2_"+model_no_r+"_r_sls_pvt")
print(schema+".DAT2_"+model_no_r+"_r_sls_pvt generated successfully")



# COMMAND ----------

# MAGIC %md
# MAGIC #### Fido Sales table 

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE mlmodel_data.DAT1_124_yuncheng_f_customer_base

# COMMAND ----------

schema='mlmodel_data'
model_no='124_yuncheng'
query_customer_base_fido="""select distinct ecid from """+schema+""".DAT1_"""+model_no+"""_f_customer_base"""
print("Customer base for fido ", query_customer_base_fido)
df2_f_customer_base=spark.sql(query_customer_base_fido) 
display(df2_f_customer_base)

# COMMAND ----------

#########################################Fido Sales table ######################################

#/*Sales clean up - channels and aggregate to ECID level for Fido*/


df2_f_sales_summary=df2_sales_join_channel.alias('a').join(df2_f_customer_base.alias('b'),col('a.enterprise_id') == col('b.ecid'),how='inner').withColumn("channel", when((upper(col("custom_channel_level_2")).isin(['CALL CENTRES','TELESALES','FIDO CALL CENTRES','ISR','UTESALES'])),"Call").when((upper(col("custom_channel_level_2")).isin(['DWBS', 'E-COMMERCE'])),"Digital").when ((upper(col("custom_channel_level_1")).like("%RETAIL")),"Retail").otherwise("other")).groupby("activity_date","enterprise_id","channel").agg(count(lit(1)).alias("sales_vol")).selectExpr("activity_date as date","enterprise_id as ecid","channel","sales_vol")

print("writing fido sales data in schema " + schema)
df2_f_sales_summary.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT2_"+model_no+"_f_sls_summary")
print(schema+".DAT2_"+model_no+"_f_sls_summary generated successfully")


#/*Pivot sales data - to prepare for joining later*/
df2_f_sales_pvt=df2_f_sales_summary.groupby('ecid')\
         .agg(F.sum('sales_vol').alias('sales_vol'),
              F.sum(when(col('channel') == 'Digital',col('sales_vol'))).alias('vol_digital'),
              F.sum(when(col('channel') == 'Call',col('sales_vol'))).alias('vol_care'),
              F.sum(when(col('channel') == 'Retail',col('sales_vol'))).alias('vol_retail'),
              F.sum(when(col('channel') == 'other',col('sales_vol'))).alias('vol_other'))


print("writing pivotted fido sales data in schema " + schema)
df2_f_sales_pvt.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".DAT2_"+model_no+"_f_sls_pvt")
print(schema+".DAT2_"+model_no+"_f_sls_pvt generated successfully")



# COMMAND ----------

test=spark.sql("select * from mlmodel_data.DAT2_124_yuncheng_f_sls_pvt")
display(test)

# COMMAND ----------


