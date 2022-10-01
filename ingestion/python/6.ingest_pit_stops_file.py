# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting pit_stops.json file from Mounted ADLS container  

# COMMAND ----------

# MAGIC %fs
# MAGIC head /mnt/formula1dlepam/raw/pit_stops.json 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

schema = StructType([StructField('raceId', IntegerType(), False),
                     StructField('driverId', IntegerType(), True),
                     StructField('stop', StringType(), True),
                     StructField('lap', IntegerType(), True),
                     StructField('time', StringType(), True),
                     StructField('duration', StringType(), True)
                    ])

# COMMAND ----------

pit_stops_df = ( spark.read.format('json')
                    .option('path', '/mnt/formula1dlepam/raw/pit_stops.json')
                    .option('multiline', True)
                    .schema(schema)
                    .load()
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and derive the required columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp

# COMMAND ----------

pit_stops_trans_df = ( pit_stops_df.withColumnRenamed('raceId', 'race_id')
                               .withColumnRenamed('driverId', 'driver_id')
                               .withColumn('ingestion_date', current_timestamp())
                   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

pit_stops_trans_df.write.mode('overwrite').parquet('dbfs:/mnt/formula1dlepam/processed/pit_stops')
