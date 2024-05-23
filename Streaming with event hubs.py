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

bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write stream in mnt

# COMMAND ----------

bronze.writeStream.option("checkpointLocation","/mnt/streaming/bronze/weather").outputMode("append").format("delta").toTable("weather")
