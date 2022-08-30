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

table_name='CLUS_123_yuncheng_FEATURES_DF'#'CLUS_test_sg_FEATURES_DF'
schema='mlmodel_data'
model_no= '123_yuncheng'
df_src = spark.sql("select * from  " + schema + "."+table_name+" order by ecid").dropDuplicates()

# COMMAND ----------

display(df_src)

# COMMAND ----------

#Preprocessing

       	 
df_src = df_src.toPandas()

df_pp=df_src[["Monthly_Intxns", "Digital_Learn", "Digital_Research","Digital_Buy", "Digital_Use_6mo","Digital_Support", "care_overall", "retail_overall"]]

# This Scaler removes the median and scales the data according to the quantile range (defaults to IQR: Interquartile Range). The IQR is the range between the 1st quartile (25th quantile) and the 3rd quartile (75th quantile).

scaler = preprocessing.RobustScaler()
scaled_robust_df = scaler.fit_transform(df_pp)
scaled_robust_df = pd.DataFrame(scaled_robust_df, columns=["Monthly_Intxns", "Digital_Learn","Digital_Research","Digital_Buy", "Digital_Use_6mo","Digital_Support","care_overall", "retail_overall"])

print(scaled_robust_df.head())


# COMMAND ----------

scaled_robust_df.head()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
sns.set(font_scale=1)
pca = PCA(n_components=len(scaled_robust_df.columns))
reduced_data = pca.fit_transform(scaled_robust_df)
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
import numpy as np
pca_num_components = 4

pca=PCA(n_components=pca_num_components)
reduced_data = pca.fit_transform(scaled_robust_df)

#TODO:Why are we using PCA when we are taking 4 PCs for 4 columns
result = pd.DataFrame(reduced_data,columns=['pca1','pca2','pca3','pca4'])

df_pca = pd.concat([df_pp, result], axis=1)

print(np.cumsum(pca.explained_variance_ratio_)) 
seeds=1
print("Generating 5 cluster  with {} seeds".format(seeds))


# COMMAND ----------

#TODO: using 3 PC as opposed to 4 mentioned above
X=df_pca[['pca1','pca2','pca3']]
#TODO: why 5 components? what about BIC and hyperparameter tuning for cov_type

gmm = GaussianMixture(n_components=5, covariance_type='full', random_state=42).fit(X)
y = gmm.predict(X)
df_src['prediction'] = y


# COMMAND ----------

#import df here
schema='mlmodel_data'
table_features='CLUS_123_yuncheng_FEATURES'#'CLUS_test_sg_FEATURES'

df = spark.sql("select * from  " + schema + "."+table_features+" order by ecid").dropDuplicates()
display(df)

# COMMAND ----------

## Convert to spark df
##TODO: start from here

sparkSchema = StructType([StructField("ECID", StringType(), True),
                          StructField("prediction", IntegerType(), True)])
df_spark = spark.createDataFrame(df_src[[ "ECID", "prediction"]], schema=sparkSchema)


clustered_data = df.join(broadcast(df_spark), on=['ECID'], how='inner')
clustered_data.createOrReplaceTempView("clustered_data")
display(clustered_data)

# COMMAND ----------

print(clustered_data.count())

# COMMAND ----------

display(clustered_data)

# COMMAND ----------

#wireless_ind,ignite_flag,Digital_Pay_6mo
std_Dev=spark.sql("select prediction,count(ECID) as ECID_CNT ,stddev(wireless_ind) as wireless_ind, stddev(Monthly_Intxns) as Monthly_Intxns,stddev(Digital_Learn) as Digital_Learn,stddev(Digital_Research) as Digital_Research,stddev(Digital_Buy) as Digital_Buy,stddev(Digital_Use_6mo) as Digital_Use_6mo,stddev(Digital_Pay_6mo) as Digital_Pay_6mo,stddev(Digital_Support) as Digital_Support,mean(care_overall) as care_overall,stddev(retail_overall) as retail_overall from clustered_data group by prediction")
display(std_Dev)

# COMMAND ----------

df_summary_6mo_nolearn = spark.sql("select prediction,count(ECID) as ECID_CNT ,mean(wireless_ind) as wireless_ind, mean(Monthly_Intxns) as Monthly_Intxns,mean(Digital_Learn) as Digital_Learn,mean(Digital_Research) as Digital_Research,mean(Digital_Buy) as Digital_Buy,mean(Digital_Use_6mo) as Digital_Use_6mo,mean(Digital_Pay_6mo) as Digital_Pay_6mo,mean(Digital_Support) as Digital_Support,mean(care_overall) as care_overall,mean(retail_overall) as retail_overall, count(ECID)/(sum(count(ECID)) over())*100 as perc_base from clustered_data group by prediction")

display(df_summary_6mo_nolearn)
df_summary_6mo_nolearn.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema + '.CLUS_'+model_no+'_summary_6mo_nolearn')
print("Writing Dataframe : " + schema + '.CLUS_'+model_no+'_summary_6mo_nolearn')
#mlmodel.clus_123

# COMMAND ----------

avg_cols = udf(lambda array: sum(array) / len(array), FloatType())
df_cluster_naming = df_summary_6mo_nolearn.withColumn("mean", avg_cols(
            array("Digital_Learn", "Digital_Research", "Digital_Buy", "Digital_Use_6mo"))).withColumn("mseg",
                                                                                                  F.col('mean') * F.col(
                                                                                                      'Monthly_Intxns')).withColumn(
            "row_num", F.row_number().over(W.orderBy(F.col('mseg').desc()))).withColumn("seg_name_en",
                                                                                 F.when(col('row_num') == 1,'Digital - Active')
                                                                                  .when(col('row_num') == 2,'Digital - Passive')
                                                                                  .when(col('row_num') == 3,'Omnichannel - Active')
                                                                                  .when(col('row_num') == 4,'Omnichannel - Passive')
                                                                                   .when(col('row_num') == 5,'Mainly Offline'))\
                                                                              .withColumn("seg_name_fr",
                                                                                 F.when(col('row_num') == 1,'Numérique - Actif')
                                                                                  .when(col('row_num') == 2,'Numérique - Pasif')
                                                                                  .when(col('row_num') == 3,'Omnicanal - Actif')
                                                                                  .when(col('row_num') == 4,'Omnicanal - Pasif')
                                                                                  .when(col('row_num') == 5,'Principalement hors ligne')) \
                                                                              .withColumn("seg_desc_en",
                                                                                  F.when(col('row_num') == 1,'Customers who mostly transact in Digital across their lifecycle. Active denotes higher than normal interaction volume usually driven by one-time payments')
                                                                                          .when(col('row_num') == 2,'Customers who mostly transact in Digital across their lifecycle. Passive denotes lower  than normal interaction volume usually linked to preauthorized payments')
                                                                                          .when(col('row_num') == 3,'Omnichannel customers who interact in multiple channels at different lifecycle stages. Active denotes higher than normal interaction volume usually driven by one-time payments')
                                                                                          .when(col('row_num') == 4,'Omnichannel customers who interact in multiple channels at different lifecycle stages. Passive denotes lower  than normal interaction volume usually linked to preauthorized payments')
                                                                                          .when(col('row_num') == 5,'Customers who prefer offline (retail and care) across their lifecycle and generally have fewer interactions/purchases.'))  \
                                                                              .withColumn("seg_desc_fr",
                                                                                          F.when(col('row_num') == 1, 'Clients qui effectuent principalement des transactions numériques tout au long de leur cycle de vie. Actif indique un volume d\'interaction supérieur à la normale, généralement provoqué par des paiements uniques.')
                                                                                          .when(col('row_num') == 2,'Clients qui effectuent principalement des transactions numériques tout au long de leur cycle de vie. Passif indique un volume d\'interaction inférieur à la normale, généralement lié aux paiements préautorisés.')
                                                                                          .when(col('row_num') == 3,'Clients omnicanaux qui interagissent sur plusieurs canaux à différentes étapes de leur cycle de vie. Actif indique un volume d\'interaction supérieur à la normale, généralement provoqué par des paiements uniques')
                                                                                          .when(col('row_num') == 4,'Clients omnicanaux qui interagissent sur plusieurs canaux à différentes étapes de leur cycle de vie. Passif indique un volume d\'interaction inférieur à la normale, généralement lié aux paiements préautorisés')
                                                                                          .when(col('row_num') == 5,'Les clients qui préfèrent le mode hors connexion (vente au détail et service client) tout au long de leur cycle de vie et ont généralement moins d\'interactions / achats.'))



# COMMAND ----------

display(df_cluster_naming)

# COMMAND ----------

df_with_clust = clustered_data.join(df_cluster_naming.select('prediction', 'row_num', 'seg_name_en','seg_name_fr','seg_desc_en','seg_desc_fr'),'prediction', 'inner')

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

display(model_output)
model_name="GMM_SG"
model_output.repartition(4).write.format("parquet").mode("overwrite").saveAsTable(schema+".CLUS_"+model_no+"_"+model_name)
print("Writing data :"+schema+".CLUS_"+model_no+"_"+model_name)



# COMMAND ----------


