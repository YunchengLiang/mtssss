# Databricks notebook source
import sys
import dateutil.parser as parser
from pyspark.sql import functions as F
from pyspark.sql.context import SQLContext
from dateutil.relativedelta import *
from datetime import datetime, timedelta
from pyspark.sql.functions import udf, array, col,broadcast,isnan
from dateutil.relativedelta import *
import calendar
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window as W
from pyspark.sql.functions import concat, col, lit
from sklearn.mixture import GaussianMixture
from sklearn import preprocessing
from sklearn.decomposition import PCA
import pandas as pd

import traceback
import logging


# COMMAND ----------

table_name='DAT5_123_yuncheng_r_merged_all'#'dat5_test_sg_r_merged_all'
schema='mlmodel_data'
model_no= '123_yuncheng'
df = spark.sql("select * from  " + schema + "."+table_name+" order by ecid").dropDuplicates()
df.persist()

print("reading dataframe")
date_str = datetime.today().strftime("%Y%m")
cols_dict = {'intxn_monthly_count': 'Monthly_Intxns',
             'digital_learn_browse': 'Digital_Browse_Products',
             'digital_learn_research': 'Digital_Research_Products',
             'digital_buy_browse': 'Digital_Browse_Pricing',
             'digital_buy_research': 'Digital_Price_and_Config',
             'digital_buy_sale': 'Digital_Buy',
             'digital_use': 'Digital_Use',
             'digital_pay': 'Digital_Pay',
             'digital_support': 'Digital_Support',
             'digital_use_6mo': 'Digital_Use_6mo',
             'digital_pay_6mo': 'Digital_Pay_6mo',
             'care_learn_browse': 'care_Browse_Products',
             'care_learn_research': 'care_Research_Products',
             'care_buy_browse': 'care_Browse_Pricing',
             'care_buy_research': 'care_Price_and_Config',
             'care_buy_sale': 'care_Buy',
             'care_get': 'care_Get',
             'care_use': 'care_Use',
             'care_pay': 'care_Pay',
             'care_support': 'care_Support',
             'care_use_6mo': 'care_Use_6mo',
             'care_pay_6mo': 'Pay_6mo',
             'retail_learn_browse': 'retail_Browse_Products',
             'retail_learn_research': 'retail_Research_Products',
             'retail_buy_browse': 'retail_Browse_Pricing',
             'retail_buy_research': 'retail_Price_and_Config',
             'retail_buy_sale': 'retail_Buy',
             'retail_get': 'retail_Get',
             'retail_use': 'retail_Use',
             'retail_pay': 'retail_Pay',
             'retail_support': 'retail_Support',
             'retail_use_6mo': 'retail_Use_6mo',
             'retail_pay_6mo': 'retail_Pay_6mo',
             'anchor_segment': 'anchor_segment',
             'ecid': 'ECID',
             'type': 'cust_type',
             'tenure_month': 'Tenure_months'
            }
df = df.select([col(c).alias(cols_dict.get(c, c)) for c in df.columns])
display(df)

# COMMAND ----------

print(df.count())

# COMMAND ----------

collist = ['Digital_Browse_Products', 'Digital_Browse_Pricing','Digital_Research_Products', 'Digital_Price_and_Config','care_browse_products', 'care_research_products', 'care_browse_pricing','care_Price_and_Config','retail_Browse_Products', 'retail_Research_Products', 'retail_Browse_Pricing','retail_Price_and_Config']

columns = df.columns
for column in columns:
  df = df.withColumn(column,F.when(col(column).isNull(),float('Nan')).otherwise(F.col(column)))
print("Replaced null with Nan")
display(df)

# COMMAND ----------

for i in collist:
  df=df.withColumn(i,F.when(col(i) == 0,float('Nan')).otherwise(col(i)))
print(" Replaced 0 with Nan")

avg_cols = udf(lambda array: sum(array) / len(array), FloatType())

min_mon_txn = df.agg({"Monthly_Intxns": 'min'}).collect()[0][0]
print("min_mon_txn : ",min_mon_txn)
max_mon_txn = df.agg({"Monthly_Intxns": 'max'}).collect()[0][0]
print("max_mon_txn : ",max_mon_txn)

col_digl = ["Digital_Browse_Products", "Digital_Browse_Pricing"]
col_digr = ["Digital_Research_Products", "Digital_Price_and_Config"]
col_carel = ["care_Browse_Products", "care_Browse_Pricing"]
col_carer = ["care_Research_Products", "care_Price_and_Config"]
col_retl = ["retail_Browse_Products", "retail_Browse_Pricing"]
col_retr = ["retail_Research_Products", "retail_Price_and_Config"]
col_care_ov = ['care_Browse_Products', 'care_Research_Products', 'care_Browse_Pricing',
             'care_Price_and_Config', 'care_Buy', 'care_Get', 'care_Use', 'care_Pay',
             'care_Support']
col_ret_ov = ['retail_Browse_Products', 'retail_Research_Products', 'retail_Browse_Pricing',
            'retail_Price_and_Config', 'retail_Buy', 'retail_Get', 'retail_Use',
            'retail_Pay', 'retail_Support']


# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn("Digital_Learn",
                           sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_digl]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_digl])) \
            .withColumn("Digital_Research",
                        sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_digr]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_digr])) \
            .withColumn("care_Learn",
                        sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_carel]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_carel])) \
            .withColumn("care_Research",
                        sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_carer]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_carer])) \
            .withColumn('retail_Learn',
                        sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_retl]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_retl])) \
            .withColumn('retail_Research',
                        sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_retr]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_retr])) \
            .withColumn('care_overall',sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_care_ov]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_care_ov]))\
            .withColumn('retail_overall',sum([F.when(~isnan(i),df[i]).otherwise(0) for i in col_ret_ov]) / sum([F.when(isnan(i), 0).otherwise(1) for i in col_ret_ov]))\
            .withColumn('Monthly_Intxns_normalized',
                        (col('Monthly_Intxns') - min_mon_txn) / (max_mon_txn - min_mon_txn))


# COMMAND ----------

df = df.fillna(0)

# COMMAND ----------

#model_no='test_sg'
#add Ignite flag, removed ignite flag
# df=df.withColumn("ignite_flag",F.when(col("cust_type")=='Ignite',1).otherwise(0))

df.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema + '.CLUS_'+model_no+'_FEATURES')	
print("Writing Dataframe : "+schema + '.CLUS_'+model_no+'_FEATURES')


# COMMAND ----------

display(df)

# COMMAND ----------

print(df.count())

# COMMAND ----------

#Preprocessing
 
df_src = df.select(df["ECID"].alias("ECID"),F.round(df["Monthly_Intxns"],4).alias("Monthly_Intxns"), F.round(df["Digital_Learn"],4).alias("Digital_Learn"), F.round(df["Digital_Research"],4).alias("Digital_Research"),F.round(df["Digital_Buy"],4).alias("Digital_Buy"), F.round(df["Digital_Use_6mo"],4).alias("Digital_Use_6mo"),F.round(df["Digital_Support"],4).alias("Digital_Support"), F.round(df["care_overall"],4).alias("care_overall"), F.round(df["retail_overall"],4).alias("retail_overall"))


# COMMAND ----------

df_src.columns

# COMMAND ----------

df_src.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema + '.CLUS_'+model_no+'_FEATURES_DF')	
print("Writing Dataframe : "+schema + '.CLUS_'+model_no+'_FEATURES_DF')


# COMMAND ----------


