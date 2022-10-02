# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting pit_stops.json file from Mounted ADLS container  

# COMMAND ----------

# MAGIC %fs
# MAGIC head /mnt/formula1dlepam/raw/pit_stops.json 

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

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
                    .option('path', f'{raw_directory}/pit_stops.json')
                    .option('multiline', True)
                    .schema(schema)
                    .load()
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and derive the required columns from the dataframe

# COMMAND ----------

pit_stops_trans_df = ( pit_stops_df.withColumnRenamed('raceId', 'race_id')
                               .withColumnRenamed('driverId', 'driver_id')
                   )
pit_stops_final_df = add_ingestion_date(pit_stops_trans_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet(f'dbfs:{processed_directory}/pit_stops')
