# Databricks notebook source
# MAGIC %md
# MAGIC #Import useful librairies

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Catalog and schema

# COMMAND ----------

try:
    spark.sql("create catalog streaming")
except:
    print("Catalog already exists")


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog streaming

# COMMAND ----------

try:
    spark.sql("create schema bronze;")
except:
    print('bronze schema already exists')

try:
    spark.sql("create schema silver;")
except:
    print('silver schema already exists')

try:
    spark.sql("create schema gold;")
except:
    print('gold schema already exists')

# COMMAND ----------

# MAGIC %md
# MAGIC #Set up azure Event Hubs

# COMMAND ----------

# MAGIC %md
# MAGIC ###Secrets

# COMMAND ----------

connectionStringEH= dbutils.secrets.get(scope='dbx-secrets-202405',key="connectionStringEH")
eventHub = dbutils.secrets.get(scope='dbx-secrets-202405',key="eventHubName")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Config

# COMMAND ----------

connectionString = connectionStringEH
eventHubName = eventHub

ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.eventHubName': eventHubName
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read stream from event hub

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema bronze

# COMMAND ----------

bronze = spark.readStream.format("eventhubs").options(**ehConf).load()

# COMMAND ----------

# bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write stream in mnt

# COMMAND ----------

bronze.writeStream.option("checkpointLocation","/mnt/streaming/bronze/weather").outputMode("append").format("delta").toTable("bronze.weather")

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define schema

# COMMAND ----------

silver_schema = StructType([
    StructField("temperature",IntegerType(),False),
    StructField("humidity",IntegerType(),False),
    StructField("windSpeed",IntegerType(),False),
    StructField("windDirection",StringType(),False),
    StructField("precipitation",IntegerType(),False),
    StructField("conditions",StructType(),False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read bronze and transform into tabulated data

# COMMAND ----------

silver = spark.readStream\
    .format("delta")\
    .table("bronze.weather")\
    .withColumn("body",col("body").cast('string'))\
    .withColumn("json_body",from_json("body",silver_schema))\
    .selectExpr("cast(body:temperature as int) as temperature",
                "cast(body:humidity as int) humidity",
                "cast(body:windSpeed as int) windSpeed",
                "cast(body:windDirection as string) windDirection",
                "cast(body:precipitation as int) precipitation",
                "cast(body:conditions as string) conditions",
                "now() as ingest_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write stream data in a db

# COMMAND ----------

silver.writeStream\
  .option("checkpointlocation","/mnt/streaming/silver/weather")\
  .outputMode("append")\
  .format("delta")\
  .toTable("silver.weather")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading
# MAGIC > Watermark 5 min

# COMMAND ----------

gold = spark.readStream\
    .format('delta')\
    .table('silver.weather')\
    .withWatermark('ingest_timestamp','5 minutes')

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation
# MAGIC > group by window 5 min
# MAGIC
# MAGIC > avg temperature,humidity,windSpeed, precipitation

# COMMAND ----------

gold_agg = gold.groupBy(window("ingest_timestamp", "5 minutes"))\
    .agg(avg("temperature").alias("avg_temperature"),
         stddev("humidity").alias("stddev_humidity"),
         min("windSpeed").alias("min_humidity"),
         max("precipitation").alias("max_precipitation"))\
    .selectExpr("window.start as startWindow","window.end endWindow","avg_temperature","stddev_humidity","min_humidity","max_precipitation")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write

# COMMAND ----------

gold_agg.writeStream\
    .option("checkpointlocation","/mnt/streaming/weather_summary")\
    .outputMode("append")\
    .format("delta")\
    .toTable("gold.weather_summary")

# COMMAND ----------

spark.readStream.table("bronze.weather").display()

# COMMAND ----------

spark.readStream.table("silver.weather").display()

# COMMAND ----------

spark.readStream.table("gold.weather_summary").display()    

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold layer correction

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.summary_weather_clone as select * from gold.weather_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table gold.weather_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.weather_summary as 
# MAGIC select window.start as startWindow,
# MAGIC window.end as endWindow,
# MAGIC * except(window)
# MAGIC from gold.summary_weather_clone
