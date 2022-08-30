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

#ingesting df_src data
table_name='CLUS_124_FEATURES_DF'
schema='mlmodel_data'

df_src = spark.sql("select * from  " + schema + "."+table_name+" order by ecid").dropDuplicates()

# COMMAND ----------

display(df_src)

# COMMAND ----------

df_src_int = df_src.toPandas()

df_pp_int=df_src_int[["Digital_Learn", "Digital_Research","Digital_Buy", "Digital_Use_6mo","Digital_Support"]]
scaler = preprocessing.RobustScaler()
scaled_robust_df_int = scaler.fit_transform(df_pp_int)
scaled_robust_df_int = pd.DataFrame(scaled_robust_df_int, columns=["Digital_Learn","Digital_Research","Digital_Buy", "Digital_Use_6mo","Digital_Support"])


print(scaled_robust_df_int.head())


# COMMAND ----------

scaled_robust_df_int.head()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
sns.set(font_scale=1)
pca = PCA(n_components=len(scaled_robust_df_int.columns))
reduced_data = pca.fit_transform(scaled_robust_df_int)
plt.figure()
plt.plot(np.cumsum(pca.explained_variance_ratio_))
plt.xlabel('Number of Components')
plt.ylabel('Variance (%)') #for each component
plt.title(' Dataset Explained Variance')
plt.show()

# COMMAND ----------

print(scaled_robust_df.shape)

# COMMAND ----------

# PCA
### Run PCA on the data and reduce the dimensions in pca_num_components dimensions
#TODO: why do we need 4 components?
pca_num_components = 4

pca=PCA(n_components=pca_num_components)
reduced_data_int = pca.fit_transform(scaled_robust_df_int)
result_int = pd.DataFrame(reduced_data_int,columns=['pca1','pca2','pca3','pca4'])

df_pca_int = pd.concat([df_pp_int, result_int], axis=1)

print(np.cumsum(pca.explained_variance_ratio_)) 
seeds=1
print("Generating 5 cluster  using GaussianMixture with {} random_state".format(seeds))
        


# COMMAND ----------

#TODO: using 3 PC as opposed to 4 mentioned above
X_int=df_pca_int[['pca1','pca2','pca3']]
gmm_int = GaussianMixture(n_components=3, covariance_type='full', random_state=42).fit(X_int)
y = gmm_int.predict(X_int)
df_src_int['prediction'] = y


# COMMAND ----------

tablename_int="CLUS_124_DF"
schema='mlmodel_data'

df_int = spark.sql("select * from  " + schema + "."+tablename_int+" order by ecid").dropDuplicates()
display(df_int)

# COMMAND ----------

## Convert to spark df
##TODO: start from here

## Convert to spark df
        
sparkSchema = StructType([StructField("ECID", StringType(), True),
                          StructField("prediction", IntegerType(), True)])
df_spark_int = spark.createDataFrame(df_src_int[[ "ECID", "prediction"]], schema=sparkSchema)

#df_spark = spark.createDataFrame(df_src[[ "ECID", "prediction"]], schema=sparkSchema)

clustered_data_int = df_int.join(broadcast(df_spark_int), on=['ECID'], how='inner')
clustered_data_int.createOrReplaceTempView("clustered_data_f_int")


# COMMAND ----------

print(clustered_data_int.count())

# COMMAND ----------

df_summary_6mo_nolearn_int = spark.sql(
"select prediction,count(ECID) as ECID_CNT ,mean(wireless_ind) as wiresless_ind,mean(Monthly_Intxns) as Monthly_Intxns,mean(Digital_Learn) as Digital_Learn,mean(Digital_Research) as Digital_Research,mean(Digital_Buy) as Digital_Buy,mean(Digital_Use_6mo) as Digital_Use_6mo,mean(Digital_Pay_6mo) as Digital_Pay_6mo,mean(Digital_Support) as Digital_Support,mean(care_overall) as care_overall,mean(retail_overall) as retail_overall, count(ECID)/(sum(count(ECID)) over())*100 as perc_base from clustered_data_f_int group by prediction")
model_no="124"
df_summary_6mo_nolearn_int.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema + '.CLUS_'+model_no+'_summary_6mo_nolearn_f_int')
print("Writing Dataframe : " + schema + '.CLUS_'+model_no+'_summary_6mo_nolearn_f_int')

#mlmodel.clus_123

# COMMAND ----------

avg_cols = udf(lambda array: sum(array) / len(array), FloatType())
df_cluster_naming_int = df_summary_6mo_nolearn_int.withColumn("mean", avg_cols(
    array("Digital_Learn", "Digital_Research", "Digital_Buy", "Digital_Use_6mo"))).withColumn("mseg",
                                                                                          F.col('mean') * F.col(
                                                                                              'Monthly_Intxns')).withColumn(
    "row_num", F.row_number().over(W.orderBy(F.col('mseg').desc()))).withColumn("seg_name_en",
                                                                         F.when(col('row_num') == 1,'Digital')
                                                                           .when(col('row_num') == 2,'Omnichannel')
                                                                           .when(col('row_num') == 3,'Mainly Offline'))\
                                                                      .withColumn("seg_name_fr",
                                                                         F.when(col('row_num') == 1,'Numérique')
                                                                          .when(col('row_num') == 2,'Omnicanal')
                                                                          .when(col('row_num') == 3,'Principalement hors ligne')) \
                                                                      .withColumn("seg_desc_en",
                                                                          F.when(col('row_num') == 1,'Customers who mostly transact in Digital across their lifecycle')
                                                                                   .when(col('row_num') == 2,'Omnichannel customers who interact in multiple channels at different lifecycle stages')
                                                                                   .when(col('row_num') == 3,'Customers who prefer offline (retail and care) across their lifecycle and generally have fewer interactions/purchases.'))  \
                                                                      .withColumn("seg_desc_fr",
                                                                                  F.when(col('row_num') == 1, 'Clients qui effectuent principalement des transactions numériques tout au long de leur cycle de vie.')
                                                                                  .when(col('row_num') == 2,'Clients omnicanaux qui interagissent sur plusieurs canaux à différentes étapes de leur cycle de vie')
                                                                                  .when(col('row_num') == 3,'Les clients qui préfèrent le mode hors connexion (vente au détail et service client) tout au long de leur cycle de vie et ont généralement moins d\'interactions / achats.'))

##Attaching cluster names to clustered dataframe
df_with_clust_int = clustered_data_int.join(df_cluster_naming_int.select('prediction', 'row_num', 'seg_name_en','seg_name_fr','seg_desc_en','seg_desc_fr'),'prediction', 'inner')
print(" Attached cluster names to Fido internet only customers")


# COMMAND ----------

#mlmodel_data.CLUS_124_features
tablename_df="CLUS_124_features"
schema='mlmodel_data'

df = spark.sql("select * from  " + schema + "."+tablename_df+" order by ecid").dropDuplicates()
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select wireless_ind,count(*) as counts from mlmodel_data.CLUS_124_features group by wireless_ind

# COMMAND ----------

#converged only
print("Converged Customers")
df_conv=df.filter(col("wireless_ind")==1)

#Preprocessing
print("Preprocessing using preprocessing.RobustScaler()")


# COMMAND ----------

display(df_conv)

# COMMAND ----------

df_src_conv = df_conv.select(df["ECID"].alias("ECID"), F.round(df["Digital_Learn"],4).alias("Digital_Learn"), F.round(df["Digital_Research"],4).alias("Digital_Research"),F.round(df["Digital_Buy"],4).alias("Digital_Buy"), F.round(df["Digital_Use_6mo"],4).alias("Digital_Use_6mo"),F.round(df["Digital_Support"],4).alias("Digital_Support"))

df_src_conv = df_src_conv.toPandas()
df_src_conv.head()

# COMMAND ----------

df_pp_conv=df_src_conv[["Digital_Learn", "Digital_Research","Digital_Buy", "Digital_Use_6mo","Digital_Support"]]
scaler = preprocessing.RobustScaler()
scaled_robust_df_conv = scaler.fit_transform(df_pp_conv)
scaled_robust_df_conv = pd.DataFrame(scaled_robust_df_conv, columns=["Digital_Learn","Digital_Research","Digital_Buy", "Digital_Use_6mo","Digital_Support"])

# PCA        
print("Reduce dimension for clustering using PCA")

### Run PCA on the data and reduce the dimensions in pca_num_components dimensions
pca_num_components = 4



# COMMAND ----------

pca=PCA(n_components=pca_num_components)
reduced_data_conv = pca.fit_transform(scaled_robust_df_conv)
result_conv = pd.DataFrame(reduced_data_conv,columns=['pca1','pca2','pca3','pca4'])

df_pca_conv = pd.concat([df_pp_conv, result_conv], axis=1)

print(pca.explained_variance_ratio_) 

print("Generating 5 cluster  using GaussianMixture with {} random_state".format(seeds))



# COMMAND ----------

X_conv=df_pca_conv[['pca1','pca2','pca3']]
gmm_conv = GaussianMixture(n_components=3, covariance_type='full', random_state=42).fit(X_conv)
y = gmm_conv.predict(X_conv)
df_src_conv['prediction'] = y

## Convert to spark df

sparkSchema = StructType([StructField("ECID", StringType(), True),
                          StructField("prediction", IntegerType(), True)])
df_spark_conv = spark.createDataFrame(df_src_conv[[ "ECID", "prediction"]], schema=sparkSchema)


clustered_data_conv = df_conv.join(broadcast(df_spark_conv), on=['ECID'], how='inner')
clustered_data_conv.createOrReplaceTempView("clustered_data_f_conv")


# COMMAND ----------

display(clustered_data_conv)

# COMMAND ----------

df_summary_6mo_nolearn_conv = spark.sql(
"select prediction,count(ECID) as ECID_CNT ,mean(wireless_ind) as wireless_ind,mean(Monthly_Intxns) as Monthly_Intxns,mean(Digital_Learn) as Digital_Learn,mean(Digital_Research) as Digital_Research,mean(Digital_Buy) as Digital_Buy,mean(Digital_Use_6mo) as Digital_Use_6mo,mean(Digital_Pay_6mo) as Digital_Pay_6mo,mean(Digital_Support) as Digital_Support,mean(care_overall) as care_overall,mean(retail_overall) as retail_overall, count(ECID)/(sum(count(ECID)) over())*100 as perc_base from clustered_data_f_conv group by prediction")
model_no="124"
df_summary_6mo_nolearn_conv.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema + '.CLUS_'+model_no+'_summary_6mo_nolearn_f_conv')
print("Writing Dataframe : " + schema + '.CLUS_'+model_no+'_summary_6mo_nolearn_f_conv')



df_cluster_naming_conv = df_summary_6mo_nolearn_conv.withColumn("mean", avg_cols(
array("Digital_Learn", "Digital_Research", "Digital_Buy", "Digital_Use_6mo"))).withColumn("mseg",
                                                                                      F.col('mean') * F.col(
                                                                                          'Monthly_Intxns')).withColumn(
"row_num", F.row_number().over(W.orderBy(F.col('mseg').desc()))).withColumn("seg_name_en",
                                                                     F.when(col('row_num') == 1,'Digital')
                                                                       .when(col('row_num') == 2,'Omnichannel')
                                                                       .when(col('row_num') == 3,'Mainly Offline'))\
                                                                  .withColumn("seg_name_fr",
                                                                     F.when(col('row_num') == 1,'Numérique')
                                                                      .when(col('row_num') == 2,'Omnicanal')
                                                                      .when(col('row_num') == 3,'Principalement hors ligne')) \
                                                                  .withColumn("seg_desc_en",
                                                                      F.when(col('row_num') == 1,'Customers who mostly transact in Digital across their lifecycle')
                                                                               .when(col('row_num') == 2,'Omnichannel customers who interact in multiple channels at different lifecycle stages')
                                                                               .when(col('row_num') == 3,'Customers who prefer offline (retail and care) across their lifecycle and generally have fewer interactions/purchases.'))  \
                                                                  .withColumn("seg_desc_fr",
                                                                              F.when(col('row_num') == 1, 'Clients qui effectuent principalement des transactions numériques tout au long de leur cycle de vie.')
                                                                              .when(col('row_num') == 2,'Clients omnicanaux qui interagissent sur plusieurs canaux à différentes étapes de leur cycle de vie')
                                                                              .when(col('row_num') == 3,'Les clients qui préfèrent le mode hors connexion (vente au détail et service client) tout au long de leur cycle de vie et ont généralement moins d\'interactions / achats.'))



# COMMAND ----------

df_with_clust_conv = clustered_data_conv.join(df_cluster_naming_conv.select('prediction', 'row_num', 'seg_name_en','seg_name_fr','seg_desc_en','seg_desc_fr'),'prediction', 'inner')
print(" Attached cluster names to Fido internet only customers")


# COMMAND ----------

###########################Merge Fido Internet and Converged
df_with_clust=df_with_clust_int.union(df_with_clust_conv)
print(" Merged Fido Internet only customers and Converged customers")

# Final Summary
df_with_clust.createOrReplaceTempView("df_with_clust")

df_summary_6mo_nolearn = spark.sql(
    "select seg_name_en,count(ECID) as ECID_CNT ,mean(Monthly_Intxns) as Monthly_Intxns,mean(Digital_Learn) as Digital_Learn,mean(Digital_Research) as Digital_Research,mean(Digital_Buy) as Digital_Buy,mean(Digital_Use_6mo) as Digital_Use_6mo,mean(Digital_Pay_6mo) as Digital_Pay_6mo,mean(Digital_Support) as Digital_Support,mean(care_overall) as care_overall,mean(retail_overall) as retail_overall, count(ECID)/(sum(count(ECID)) over())*100 as perc_base from df_with_clust group by seg_name_en")

df_summary_6mo_nolearn.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema + '.CLUS_'+model_no+'_summary_6mo_nolearn')
print("Writing Dataframe : " + schema + '.CLUS_'+model_no+'_summary_6mo_nolearn')


# COMMAND ----------

display(df_summary_6mo_nolearn)

# COMMAND ----------

df_preferred_channel = df_with_clust.\
  withColumn('preferred_channel_purchase_cycle_en',
            F.when((col('digital_purchase') > 0) & (col('digital_purchase') > col('care_purchase')) & (col('digital_purchase') > col('retail_purchase')), 'Digital')
            .when((col('care_purchase') > 0) & (col('care_purchase') > col('digital_purchase')) & (col('care_purchase') > col('retail_purchase')),'Care')
            .when((col('retail_purchase') > 0) & (col('retail_purchase') > col('digital_purchase')) & (col('retail_purchase') > col('care_purchase')), 'Retail').otherwise('None')) \
  .withColumn('preferred_channel_purchase_cycle_fr',
             F.when((col('digital_purchase') > 0) & (col('digital_purchase') > col('care_purchase')) & (col('digital_purchase') > col('retail_purchase')), 'Numérique')
              .when((col('care_purchase') > 0) & (col('care_purchase') > col('digital_purchase')) & (col('care_purchase') > col('retail_purchase')), 'Service client')
              .when((col('retail_purchase') > 0) & (col('retail_purchase') > col('digital_purchase')) & (col('retail_purchase') > col('care_purchase')), 'Vente au détail').otherwise('aucun')) \
          .withColumn('preferred_channel_sale_en',
             F.when((col('digital_buy') > 0) & (col('digital_buy') > col('care_buy')) & (col('digital_buy') > col('retail_buy')), 'Digital')
              .when((col('care_buy') > 0) & (col('care_buy') > col('digital_buy')) & (col('care_buy') > col('retail_buy')), 'Care')
              .when((col('retail_buy') > 0) & (col('retail_buy') > col('digital_buy')) & (col('retail_buy') > col('care_buy')), 'Retail').otherwise('None')) \
  .withColumn('preferred_channel_sale_fr',
              F.when((col('digital_buy') > 0) & (col('digital_buy') > col('care_buy')) & (col('digital_buy') > col('retail_buy')), 'Numérique')
              .when((col('care_buy') > 0) & (col('care_buy') > col('digital_buy')) & (col('care_buy') > col('retail_buy')), 'Service client')
              .when((col('retail_buy') > 0) & (col('retail_buy') > col('digital_buy')) & (col('retail_buy') > col('care_buy')), 'Vente au détail').otherwise('aucun')) \
  .withColumn('preferred_channel_service_en',
              F.when((col('digital_service') > 0) & (col('digital_service') > col('care_service')) & (col('digital_service') > col('retail_service')), 'Digital')
               .when((col('care_service') > 0) & (col('care_service') > col('digital_service')) & (col('care_service') > col('retail_service')), 'Care')
               .when((col('retail_service') > 0) & (col('retail_service') > col('digital_service')) & (col('retail_service') > col('care_service')), 'Retail').otherwise('None')) \
  .withColumn('preferred_channel_service_fr',
          F.when((col('digital_service') > 0) & (col('digital_service') > col('care_service')) & (col('digital_service') > col('retail_service')), 'Numérique')
          .when((col('care_service') > 0) & (col('care_service') > col('digital_service')) & (col('care_service') > col('retail_service')), 'Service client')
          .when((col('retail_service') > 0) & (col('retail_service') > col('digital_service')) & (col('retail_service') > col('care_service')), 'Vente au détail').otherwise('aucun'))\
  .withColumn("Preferred_channel_purchase_num",F.when((col("preferred_channel_purchase_cycle_en") == "Digital"), 1)
                                      .when((col("preferred_channel_purchase_cycle_en") == "Care"), 2)
                                      .when((col("preferred_channel_purchase_cycle_en") == "Retail"), 3)
                                       .when((col("preferred_channel_purchase_cycle_en") == "None"), 4))   \
  .withColumn("Preferred_channel_sale_num", F.when((col("preferred_channel_sale_en") == "Digital"), 1)
                                            .when((col("preferred_channel_sale_en") == "Care"), 2)
                                           .when((col("preferred_channel_sale_en") == "Retail"), 3)
                                           .when((col("preferred_channel_sale_en") == "None"), 4)) \
  .withColumn("Preferred_channel_service_num",F.when((col("preferred_channel_service_en") == "Digital"), 1)
                                            .when((col("preferred_channel_service_en") == "Care"), 2)
                                           .when((col("preferred_channel_service_en") == "Retail"), 3)
                                           .when((col("preferred_channel_service_en") == "None"), 4))


df_purchase_cycle = df_preferred_channel.\
  withColumn('preferred_channel_en', concat(lit('Purchase Cycle:<'),col('preferred_channel_purchase_cycle_en') ,lit('>; Sale:<'),col('preferred_channel_sale_en'),lit('>; Service:<'),col('preferred_channel_service_en'),lit('>'))) \
  .withColumn('preferred_channel_fr', concat(lit('Cycle d\'achat:<'),col('preferred_channel_purchase_cycle_fr') ,lit('>; Sale:<'),col('preferred_channel_sale_fr'),lit('>; Service:<'),col('preferred_channel_service_fr'),lit('>')))

model_output = df_purchase_cycle.select(col("ECID"),col("row_num").alias("seg_cd"),col("seg_name_en"),col("seg_name_fr"),col("seg_desc_en"),col("seg_desc_fr"),col("preferred_channel_en").alias("seg_offer_en"),col("preferred_channel_fr").alias("seg_offer_fr"),col("Preferred_channel_purchase_num").alias("pref_pur_num"),col("Preferred_channel_sale_num").alias("pref_chan_num"),col("Preferred_channel_service_num").alias("pref_service_num"))

model_name='test_sg_f'
model_output.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".CLUS_"+model_no+"_"+model_name)
print("Writing Dataframe : " + schema + '.CLUS_'+model_no+'_'+model_name)


# COMMAND ----------

display(model_output)
