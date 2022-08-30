# Databricks notebook source
import sys
import random
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from datetime import datetime
import pyspark.sql.functions
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, lit
import random
import time
# Change the init file
import traceback
import logging



# COMMAND ----------

output=spark.sql("Select distinct model_fact_name,model_name,run_date from mlmodel.ml_model_output_fct where model_name='RES_DAAO_CL' and model_fact_name='RES_DAAO_CL_NAME_EN' order by 3 desc")
display(output)
#2022-05-27
#2020-07-21

# COMMAND ----------

output=spark.sql("Select * from mlmodel.ml_model_output_fct where model_name='RES_DAAO_CL' and model_fact_name='RES_DAAO_CL_NAME_EN' and run_date='2022-06-22' and CUSTOMER_ID in (select * from mlmodel_data.dat1_123_yuncheng_rogers_overlap) ")
display(output)

# COMMAND ----------

output.count()#1688957


# COMMAND ----------

output=output.toPandas()

# COMMAND ----------

grp_output= output.groupby('MODEL_OUTPUT_VALUE')['CUSTOMER_ID'].size().reset_index()

# COMMAND ----------

display(grp_output)

# COMMAND ----------

output=spark.sql("Select * from mlmodel.ml_model_output_fct where model_name='RES_DAAO_CL' and model_fact_name='RES_DAAO_CL_NAME_EN' and run_date='2022-06-22'")
display(output)
print(output.count())

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mlmodel_data.CLUS_123_yuncheng_GMM_SG 

# COMMAND ----------

# MAGIC %sql
# MAGIC select seg_name_en, count(distinct ECID) from mlmodel_data.CLUS_123_yuncheng_GMM_SG group by seg_name_en

# COMMAND ----------

# Calculate the skewness, kurtosis and mean and std dev
dig_active=output[output['MODEL_OUTPUT_VALUE']=='Digital - Active']['CUSTOMER_ID'].reset_index()
dig_active_grp=dig_active.groupby('index')['CUSTOMER_ID'].size()


# COMMAND ----------

dig_active_grp.sum()

# COMMAND ----------


