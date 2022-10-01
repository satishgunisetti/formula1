# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting drivers.json file from Mounted ADLS container  

# COMMAND ----------

# MAGIC %fs
# MAGIC head /mnt/formula1dlepam/raw/drivers.json

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType([StructField('forename', StringType(), False),
                          StructField('surname', StringType(), True)
                         ])

# COMMAND ----------

schema = StructType([StructField('driverId', IntegerType(), False),
                     StructField('driverRef', StringType(), True),
                     StructField('number', IntegerType(), True),
                     StructField('code', StringType(), True),
                     StructField('name', name_schema),
                     StructField('dob', DateType(), True),
                     StructField('nationality', StringType(), True),
                     StructField('url', StringType(), True)
                    ])

# COMMAND ----------

drivers_df = ( spark.read.format('json')
                    .option('path', '/mnt/formula1dlepam/raw/drivers.json')
                    .schema(schema)
                    .load()
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and derive the required columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import array, array_join, col, current_timestamp

# COMMAND ----------

drivers_trans_df = ( drivers_df.withColumnRenamed('driverId', 'driver_id')
                               .withColumnRenamed('driverRef', 'driver_ref')
                               .withColumn('name', array_join(array(col('name.forename'), col('name.surname')), ' '))
                               .withColumn('ingestion_date', current_timestamp())
                               .drop('url')
                   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

drivers_trans_df.write.mode('overwrite').parquet('dbfs:/mnt/formula1dlepam/processed/drivers')
