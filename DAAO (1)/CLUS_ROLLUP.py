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

timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
run_date = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')

# COMMAND ----------

def roll_up(inputData, model_name, modelStatus, model_version, model_level, level):
    print("*******starting roll up ***********")
    inputData = inputData.filter(col(model_level) != 0)
    df1 = inputData.select(model_name,model_name + '_NAME_EN',model_name + '_NAME_FR', model_name + '_DESC_EN',model_name + '_DESC_FR').distinct()
    df2_min = inputData.groupby(model_level).agg(F.min(model_name).alias(model_name),F.min("pref_pur_num").alias("pref_pur_num"),F.min("pref_chan_num").alias("pref_chan_num"),F.min("pref_service_num").alias("pref_service_num"))
    df2_com = df2_min.join(df1,model_name,how="inner").withColumnRenamed(model_level,'customer_id')
    pref_chan_dict_en = { 1 : "Digital",
                          2 :"Care",
                          3: "Retail",
                          4: "None"}
    map_func_en = udf(lambda row: pref_chan_dict_en.get(row, row))
    pref_chan_dict_fr = {1 : "Numérique",
                         2 :"Service client",
                         3 : "Vente au détail",
                          4: "aucun"}
    map_func_fr = udf(lambda row: pref_chan_dict_fr.get(row, row))
    df2_com= df2_com.withColumn( model_name+"_OFFR_EN", concat(lit('Purchase Cycle:<'),map_func_en(col("pref_pur_num")),lit('>; Sale:<'),map_func_en(col("pref_chan_num")),lit('>; Service:<'),map_func_en(col("pref_service_num")),lit('>')))\
                     .withColumn( model_name+"_OFFR_FR", concat(lit('Cycle d\'achat:<'),map_func_fr(col("pref_pur_num")),lit('>; Sale:<'),map_func_fr(col("pref_chan_num")),lit('>; Service:<'),map_func_fr(col("pref_service_num")),lit('>')))\
                     .drop("pref_pur_num","pref_chan_num","pref_service_num")
    df2_com = df2_com.withColumn("model_level",lit(level)).withColumn("model_version",lit(model_version))\
             .withColumn("etl_insert_ts",lit(timestamp).cast("timestamp"))\
             .withColumn("model_status",lit(modelStatus)).withColumn("run_date",lit(run_date).cast("date")) \
             .withColumn("output_type",lit("Segment_Output")).withColumn("model_name",lit(model_name))

    df_sub = df2_com.select('customer_id', 'model_level', 'run_date', 'output_type', 'model_version',
                                 'model_status', 'model_name', 'etl_insert_ts')
    df3 = df2_com.select(df2_com.columns[1], lit(df2_com.columns[0]).alias('model_fact_name'),
                     df2_com[df2_com.columns[0]].alias('model_output_value').cast('String'))
    for i in df2_com.columns[2:8]:
        df3 = df3.unionAll(df2_com.select(df2_com.columns[1], lit(i), df2_com[i]))
    df_final = df_sub.join(df3, 'customer_id', how='inner')
    df_final1 = df_final.select("model_fact_name", "model_level", "output_type", "model_output_value", "customer_id",
                                "etl_insert_ts", "run_date", "model_name", "model_status", "model_version")
    print("Generated  Dataframe at ", level)
    return df_final1


# COMMAND ----------

def rollup( schema, ml_schema, model_no ):
  appName = "CLUS_ROLLUP_"+model_no
  print(appName)
  ###Getting Model_name & model_versionsion from ModelNameID
  ctnFlag = 0
  banFlag = 0
  canFlag = 0
  ecidFlag = 0

  model_ref = spark.sql("select roll_up_level,model_status,model_name_id from mlmodel.model_ref where model_no=" + model_no + " and ACTIVE_SCORE=1")
  modelStatus = model_ref.select('model_status').collect()[0][0]
  roll_up_level = model_ref.select('roll_up_level').collect()[0][0]
  model_name_id = model_ref.select('model_name_id').collect()[0][0]
  model_name=model_name_id.rsplit("_",1)[0]
  model_version=model_name_id.rsplit("_",1)[1]
  print("Model Name Id : ",model_name_id)
  print("Model Version is : " + str(model_version))
  print("model name is  : " + str(model_name))
  print("Roll Up Level : ",roll_up_level)
  print("Model Status : ",modelStatus)
  print("***** input table name  ****" ,"clus_"+model_no+"_"+model_name )
  inputDF = spark.sql("select distinct * from " + schema + ".CLUS_" + model_no+"_"+model_name )
  counts = model_ref.count()

  if counts > 1:
      print("Model  has more than 1 row for model" + str(model_name))
      raise Exception("Model  has more than 1 row for model" + str(model_name))
  if counts == 0:
      print("Model ref has 0 rows for model :" + str(model_name))
      raise Exception("Model ref has 0 rows for model :" + str(model_name))


  col_dict = {'seg_cd': model_name,
              'seg_name_en': model_name+'_NAME_EN',
              'seg_desc_en': model_name+'_DESC_EN',
              'seg_name_fr' : model_name+'_NAME_FR',
              'seg_desc_fr' : model_name+'_DESC_FR',
               'pref_pur_num':'pref_pur_num',
               'pref_chan_num':'pref_chan_num',
                'pref_service_num':'pref_service_num'}
  inputDF = inputDF.select([col(c).alias(col_dict.get(c.lower(), c)) for c in inputDF.columns]).drop("seg_offer_en","seg_offer_fr")

  ml_cols = "model_fact_name|model_level|output_type|model_output_value|customer_id|etl_insert_ts|run_date|model_name|model_status|model_version"
  mlDf = spark.createDataFrame([tuple('' for i in ml_cols.split("|"))], ml_cols.split("|")).where("1=0")
  ###Getting model levels
  for l in roll_up_level.split(","):
      if l.strip() == "ECID":
          ecidFlag = 1
      elif l.strip() == "BAN":
          banFlag = 1
      elif l.strip() == "CTN":
          ctnFlag = 1
      elif l.strip() == "CAN":
          canFlag = 1

  if ecidFlag == 1:
      model_level = 'ecid'
      level = "ECID"
      print("******Starting ecid Roll UP at " + level + "**************")
      df_ecid = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
      mlDf = mlDf.union(df_ecid)
  if banFlag == 1:
      model_level = 'customer_ban'
      level = "BAN"
      print("******Starting ban Roll UP*****" + level + "*********")
      df_ban = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
      mlDf = mlDf.union(df_ban)
  if canFlag == 1:
      model_level = 'CAN'
      level = "CAN"
      print("******Starting can Roll UP at " + level + "**************")
      df_can = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
      mlDf = mlDf.union(df_can)
  if ctnFlag == 1:
      model_level = 'subscriber_no'
      level = "CTN"
      print("******Starting ctn Roll UP at " + level + "**************")
      df_ctn = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
      mlDf = mlDf.union(df_ctn)

  print("Writing Dataframe")
  mlDf.repartition(4).write.format("parquet").mode("overwrite").insertInto(ml_schema + '.ml_model_output_fct')
  print("Written Dataframe successfully")


# COMMAND ----------


model_no="123"
stg_schema = 'mlmodel_data'
ml_schema = 'mlmodel'
model_no_f = "124"

#rollup(stg_schema,ml_schema,model_no)

# COMMAND ----------

ctnFlag = 0
banFlag = 0
canFlag = 0
ecidFlag = 0
model_ref = spark.sql("select roll_up_level,model_status,model_name_id from mlmodel.model_ref where model_no=" + model_no + " and ACTIVE_SCORE=1")
modelStatus = model_ref.select('model_status').collect()[0][0]
roll_up_level = model_ref.select('roll_up_level').collect()[0][0]
model_name_id = model_ref.select('model_name_id').collect()[0][0]
model_name=model_name_id.rsplit("_",1)[0]
model_version=model_name_id.rsplit("_",1)[1]
print("Model Name Id : ",model_name_id)
print("Model Version is : " + str(model_version))
print("model name is  : " + str(model_name))
print("Roll Up Level : ",roll_up_level)
print("Model Status : ",modelStatus)
print("***** input table name  ****" ,"clus_"+model_no+"_"+model_name )
#clus_test_sg_gmm_sg
inputDF = spark.sql("select distinct * from " + "mlmodel_data" + ".CLUS_123_yuncheng_GMM_SG" )
counts = model_ref.count()
if counts > 1:
      print("Model  has more than 1 row for model" + str(model_name))
      raise Exception("Model  has more than 1 row for model" + str(model_name))
if counts == 0:
    print("Model ref has 0 rows for model :" + str(model_name))
    raise Exception("Model ref has 0 rows for model :" + str(model_name))
col_dict = {'seg_cd': model_name,
              'seg_name_en': model_name+'_NAME_EN',
              'seg_desc_en': model_name+'_DESC_EN',
              'seg_name_fr' : model_name+'_NAME_FR',
              'seg_desc_fr' : model_name+'_DESC_FR',
               'pref_pur_num':'pref_pur_num',
               'pref_chan_num':'pref_chan_num',
                'pref_service_num':'pref_service_num'}
inputDF = inputDF.select([col(c).alias(col_dict.get(c.lower(), c)) for c in inputDF.columns]).drop("seg_offer_en","seg_offer_fr")

ml_cols = "model_fact_name|model_level|output_type|model_output_value|customer_id|etl_insert_ts|run_date|model_name|model_status|model_version"
mlDf = spark.createDataFrame([tuple('' for i in ml_cols.split("|"))], ml_cols.split("|")).where("1=0")

###Getting model levels
for l in roll_up_level.split(","):
  if l.strip() == "ECID":
      ecidFlag = 1
  elif l.strip() == "BAN":
      banFlag = 1
  elif l.strip() == "CTN":
      ctnFlag = 1
  elif l.strip() == "CAN":
      canFlag = 1


if ecidFlag == 1:
    model_level = 'ecid'
    level = "ECID"
    print("******Starting ecid Roll UP at " + level + "**************")
    df_ecid = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
    mlDf = mlDf.union(df_ecid)
    display(mlDf)
if banFlag == 1:
    model_level = 'customer_ban'
    level = "BAN"
    print("******Starting ban Roll UP*****" + level + "*********")
    df_ban = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
    mlDf = mlDf.union(df_ban)
if canFlag == 1:
    model_level = 'CAN'
    level = "CAN"
    print("******Starting can Roll UP at " + level + "**************")
    df_can = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
    mlDf = mlDf.union(df_can)
if ctnFlag == 1:
    model_level = 'subscriber_no'
    level = "CTN"
    print("******Starting ctn Roll UP at " + level + "**************")
    df_ctn = roll_up(inputDF, model_name, modelStatus, model_version, model_level, level)
    mlDf = mlDf.union(df_ctn)

# print("Writing Dataframe")
# display(ml_df)
# #   mlDf.repartition(4).write.format("parquet").mode("overwrite").insertInto(ml_schema + '.ml_model_output_fct')
# print("Written Dataframe successfully")



# COMMAND ----------

roll_up_level

# COMMAND ----------

# mlDf
pd_mldf=mlDf.toPandas()
pd_mldf.head()

# COMMAND ----------



# COMMAND ----------

display(pd_mldf[(pd_mldf['model_fact_name']=='RES_DAAO_CL_NAME_EN')].groupby('model_output_value')['customer_id'].size().reset_index())

# COMMAND ----------

output=spark.sql("Select model_fact_name,model_name,max(run_date) from mlmodel.ml_model_output_fct where model_name='RES_DAAO_CL' and model_fact_name='RES_DAAO_CL_NAME_EN' group by 1,2")
display(output)



# COMMAND ----------

output=spark.sql("Select model_fact_name,model_name,min(run_date) from mlmodel.ml_model_output_fct where model_name='RES_DAAO_CL' group by 1,2")
display(output)

# COMMAND ----------


