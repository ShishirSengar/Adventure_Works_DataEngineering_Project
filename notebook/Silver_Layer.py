# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.storageforaw.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storageforaw.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storageforaw.dfs.core.windows.net", "096b1dc5-****-4df0-****-e9f7a07b6633")
spark.conf.set("fs.azure.account.oauth2.client.secret.storageforaw.dfs.core.windows.net","aYu8Q~***~UD92zTuGu82uw_zKJxJila2sPvmb0Y")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storageforaw.dfs.core.windows.net", "https://login.microsoftonline.com/78a8d6c4-****-4ea1-****-dba2e0695e47/oauth2/token")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Data

# COMMAND ----------

df_cal = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Calendar")


# COMMAND ----------

df_cus = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Customers")


# COMMAND ----------

df_pro_cat = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Product_Categories")



# COMMAND ----------

df_pro_subcat = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Product_Subcategories")



# COMMAND ----------

df_products = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Products")


# COMMAND ----------

df_returns = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Returns")


# COMMAND ----------

df_sales_2015 = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Sales_2015")


# COMMAND ----------

df_sales_2016 = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Sales_2016")


# COMMAND ----------

df_sales_2017 = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Sales_2017")


# COMMAND ----------

df_sales = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@storageforaw.dfs.core.windows.net/Sales*')

# COMMAND ----------

df_territories = spark.read.format("csv").option("header", "true").option("inferSchema", True).load("abfss://bronze@storageforaw.dfs.core.windows.net/Territories")


# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

df_cal = df_cal.withColumn('Month', month(col('Date')))\
    .withColumn('Year', year(col('Date')))
df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calender

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Calender")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn('FullName', concat(col('Prefix'), lit(' '), col('FirstName'),lit(' '), col('LastName'))).display()


# COMMAND ----------

# DBTITLE 1,Cell 24
df_cus = df_cus.withColumn('FullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Customer")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product_Category

# COMMAND ----------

df_pro_cat.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Product_Category")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product_Subcategory

# COMMAND ----------

df_pro_subcat.display()

# COMMAND ----------

df_pro_subcat.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Product_SubCategory")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products = df_products.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
    .withColumn('ProductName', split(col('ProductName'), ' ')[0])

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Returns

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_returns.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Territories

# COMMAND ----------

df_territories.display()

# COMMAND ----------

df_territories.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Territories")\
            .save()

# COMMAND ----------

df_sales_2015.display()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales = df_sales.withColumn('Multiply', col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@storageforaw.dfs.core.windows.net/Sales")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales Analysis

# COMMAND ----------

df_sales.groupBy("OrderDate").agg(count('OrderNumber').alias('TotalOrders')).display()

# COMMAND ----------

df_pro_cat.display()

# COMMAND ----------

df_territories.display()