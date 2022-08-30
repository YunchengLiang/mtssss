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
import openpyxl

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

schema='mlmodel'#old version
query_df7="""select * from """+schema+""".dat3_126_web_common where hash_ban is not null and date_time >= to_date('"""+twoYearStart+"""') and  date_time< to_date('"""+twoYearEnd+"""')"""
print("Executing web data from wireless segment ---------------------->",query_df7)
df7=spark.sql(query_df7)
display(df7)

# COMMAND ----------

schema='mlmodel_data'#anthoy
query_df8="""select * from """+schema+""".dat3_126_web_common where hash_ban is not null and date_time >= to_date('"""+twoYearStart+"""') and  date_time< to_date('"""+twoYearEnd+"""')"""
print("Executing web data from wireless segment ---------------------->",query_df8)
df8=spark.sql(query_df8)
display(df8)

# COMMAND ----------

display(df7.describe())

# COMMAND ----------

display(df8.describe())

# COMMAND ----------

import pandas as pd
import numpy as np
df8col=list(set(df8.columns)-set(['hash_ban','date_time','exec_run_id']))
df8col=list(map(lambda x: str.upper(x),df8col))
df8pd=df8.sample(0.5,119).select(df8col).toPandas()
df7col=list(set(df7.columns)-set(['DATE_TIME','HASH_BAN','exec_run_id','az_insert_ts','az_update_ts']))
df7pd=df7.sample(0.5,119).select(df7col).toPandas()
print(df8col,df7col)

# COMMAND ----------

from statsmodels.stats.weightstats import ztest as ztest
ztest_p_value_list=[]
for feature in df7pd.columns:
    baseline_feature = list(df7pd[feature])
    compare_feature = list(df8pd[feature])
    ztest_p_value_list.append(ztest(baseline_feature, compare_feature)[1])
features_ztest_df = pd.DataFrame(data=df7pd.columns ,columns=["feature"])
features_ztest_df["ztest_p_alue"] =np.array(ztest_p_value_list) 
features_ztest_df["significance"] =np.where(features_ztest_df["ztest_p_alue"]<0.05,"significant","insignificant")
features_ztest_df

# COMMAND ----------

summaryold=df7.describe().toPandas()
summarynew=df8.describe().toPandas()
summaryold=summaryold.set_index("summary")
summarynew=summarynew.set_index("summary")


# COMMAND ----------

summaryold=summaryold.drop(columns=["exec_run_id"])
summarynew.columns=[str.upper(i)+'_new' for i in summarynew.columns]
summaryold.columns=[i+'_old' for i in summaryold.columns]

# COMMAND ----------

result=pd.concat([summaryold,summarynew],axis=1)
result=result.sort_index(ascending=True,axis=1)
result

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

sparkDF=spark.createDataFrame(result) 
sparkDF.printSchema()
display(sparkDF)

# COMMAND ----------

result.to_excel("result.xlsx")

# COMMAND ----------


