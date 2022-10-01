# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting results.json file from Mounted ADLS container  

# COMMAND ----------

# MAGIC %fs
# MAGIC head /mnt/formula1dlepam/raw/results.json

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

schema = StructType([StructField('resultId', IntegerType(), False),
                     StructField('raceId', IntegerType(), True),
                     StructField('driverId', IntegerType(), True),
                     StructField('constructorId', IntegerType(), True),
                     StructField('number', IntegerType(), True),
                     StructField('grid', IntegerType(), True),
                     StructField('position', IntegerType(), True),
                     StructField('positionText', StringType(), True),
                     StructField('positionOrder', IntegerType(), True),
                     StructField('points', DoubleType(), True),
                     StructField('laps', IntegerType(), True),
                     StructField('time', StringType(), True),
                     StructField('milliseconds', IntegerType(), True),
                     StructField('fastestLap', IntegerType(), True),
                     StructField('rank', IntegerType(), True),
                     StructField('fastestLapTime', StringType(), True),
                     StructField('fastestLapSpeed', StringType(), True),
                     StructField('statusId', IntegerType(), True)
                    ])

# COMMAND ----------

results_df = ( spark.read.format('json')
                    .option('path', '/mnt/formula1dlepam/raw/results.json')
                    .schema(schema)
                    .load()
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and derive the required columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp

# COMMAND ----------

results_trans_df = ( results_df.withColumnRenamed('resultId', 'result_id')
                               .withColumnRenamed('raceId', 'race_id')
                               .withColumnRenamed('driverId', 'driver_id')
                               .withColumnRenamed('constructorId', 'constructor_id')
                               .withColumnRenamed('positionText', 'position_text')
                               .withColumnRenamed('positionOrder', 'position_order')
                               .withColumnRenamed('fastestLap', 'fastest_lap')
                               .withColumnRenamed('fastestLapTime', 'fastest_lap_time')
                               .withColumnRenamed('fastestLapSpeed', 'fastest_lap_field')
                               .withColumn('ingestion_date', current_timestamp())
                               .drop('statusIdstatusId')
                   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

results_trans_df.write.mode('overwrite').parquet('dbfs:/mnt/formula1dlepam/processed/results')
