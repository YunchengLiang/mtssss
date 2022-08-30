# Databricks notebook source
from __future__ import print_function
import sys
print(sys.argv, len(sys.argv))

# COMMAND ----------

print(sys.argv[2])

# COMMAND ----------

import sys
import logging
import random
import logging.handlers
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import DAT3_123_DAAO_WEB_DATA
import DAT4_123_DAAO_OFFLINE_DATA
import DAT5_123_DAAO_MERGE
import CLUS_123_DAAO
import CLUS_ROLLUP
import traceback
import logging

try:
    print("Entered Init")  	
    model_name_id=sys.argv[1].upper()
    model_no=sys.argv[2] 
    moduleName = sys.argv[3].upper()
    root_dir=sys.argv[4]
    stg_schema = sys.argv[5]
    ml_schema = sys.argv[6]
    model_no_f = "124"

    OpenYrMth = datetime.today().strftime("%y"+"%m"+"%d")
    print("OpenYrMth is :", OpenYrMth)
    if moduleName == '':
        raise Exception("Module Name argument can't be null")
    if model_name_id == '':
        raise Exception("Model_name_id argument can't be null")
    if model_no == '':
        raise Exception("Model No. argument can't be null")
    if root_dir == '':
        raise Exception("Root Dir argument can't be null")
    if stg_schema == '':
        ("Schema argument is null using mlmodel")
        stg_schema='mlmodel_etl'
    if ml_schema == '':
        ("Schema argument is null using mlmodel")
        ml_schema = 'mlmodel'

    #sys.path.insert(1,root_dir+'/scripts/python/common_module') 
    #print("Sys path is " , sys.path)
    stg_schema='mlmodel'
    ml_schema = 'mlmodel'
    model_path = root_dir+'/scripts/python/'+model_name_id
    filePath = root_dir+"/TMP/SPARK_RUN_STATUS_"+moduleName+".txt"
    model_name_id='RES_DAAO_CL_1'
    model_name=model_name_id.rsplit("_",1)[0]
    model_version=model_name_id.rsplit("_",1)[1]
    print("Model Name is ",model_name)

except BaseException as e:
    logging.error(traceback.format_exc())
    setExitCode("1")
    print(e.__doc__)



def chktbl(schema, tname, sql):
    tables = sql.tableNames(schema)
    if tname.lower() in tables:
        print("table " + tname + " exists")
    else:
        print("table " + tname + " not exists")
        setExitCode("1")
        exit(1)

def setExitCode(code):
    f = open(filePath,'w+')
    f.write(code)
    f.close()

def getSparkSession():
    conf = SparkConf() \
        .setAppName(moduleName) \
        .set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true") \
        .set("spark.ui.port", (4040 + random.randint(1, 1000))) \
        .set("hive.exec.dynamic.partition.mode", "nonstrict") \
        .set("spark.sql.orc.filterPushdown", "true") \
        .set("spark.sql.hive.convertMetastoreOrc","false") \
        .set("hive.exec.dynamic.partition", "true") \
        .set("spark.shuffle.encryption.enabled", "false") \
        .set("spark.kryoserializer.buffer.max", "2000") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .set("spark.sql.broadcastTimeout", "2000") \
        .set("spark.driver.maxResultSize", "4G") \
        .set("hive.optimize.sort.dynamic.partition", "true") \
        .set("hive.auto.convert.join", "true") \
        .set("spark.sql.tungsten.enabled", "true") \
        .set("spark.sql.crossJoin.enabled", "true") \
        .set("spark.dynamicAllocation.enabled", "true") \
        .set("spark.shuffle.service.enabled", "true") \
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .set("spark.sql.hive.verifyPartitionPath","false") \
        .set("spark.hive.support.enabled", "true") \
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")

    ss = SparkSession \
        .builder \
        .appName(moduleName) \
        .master("yarn") \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    ss.sparkContext.setLogLevel('WARN')
    return ss

def main(model_name_id,model_no,moduleName,root_dir,stg_schema):
    try:
        setExitCode("0")
        if moduleName == 'DAT3_123_DAAO_WEB_DATA':
            print("Starting DAT3_126_DAAO_INTXN_DATA processing for ")
            DAT3_123_DAAO_WEB_DATA.run_app(stg_schema,model_no)
        elif moduleName == 'DAT4_123_DAAO_OFFLINE_DATA':
            print("Starting DAT4_123_DAAO_OFFLINE_DATA processing for ")
            DAT4_123_DAAO_OFFLINE_DATA.run_app(stg_schema,model_no)
        elif moduleName == 'DAT5_123_DAAO_MERGE':
            print("Starting DAT5_123_DAAO_MERGE processing for ")
            DAT5_123_DAAO_MERGE.run_app(stg_schema,model_no)
        elif moduleName == 'CLUS_123_DAAO':
            table_name="DAT5_"+model_no+"_r_merged_all"
            print("Starting CLUS_123 processing for ",table_name)
            CLUS_123_DAAO.run_app(stg_schema,table_name,model_no,42,model_name)
        elif moduleName == 'CLUS_ROLLUP_123':
            print("Starting CLUS_ROLLUP_123 processing for ")			
            CLUS_ROLLUP.rollup(stg_schema,ml_schema,model_no)
    except Exception as e:
        logging.error(traceback.format_exc())
        setExitCode("1")
        print(e.__doc__)


if __name__ == '__main__':
    print("Executing Main " + moduleName)
    main(model_name_id,model_no,moduleName,root_dir,stg_schema)


# COMMAND ----------


