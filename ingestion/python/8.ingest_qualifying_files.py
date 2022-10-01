# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting json files of qualifying folder from Mounted ADLS container  

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

schema = StructType([StructField('qualifyId', IntegerType(), False),
                     StructField('raceId', IntegerType(), True),
                     StructField('driverId', IntegerType(), True),
                     StructField('constructorId', IntegerType(), True),
                     StructField('number', IntegerType(), True),
                     StructField('position', IntegerType(), True),
                     StructField('q1', StringType(), True),
                     StructField('q2', StringType(), True),
                     StructField('q3', StringType(), True)
                    ])

# COMMAND ----------

qualifying_df = ( spark.read.format('json')
                    .option('path', '/mnt/formula1dlepam/raw/qualifying')
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

qualifying_trans_df = ( qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')
                               .withColumnRenamed('raceId', 'race_id')
                               .withColumnRenamed('driverId', 'driver_id')
                               .withColumnRenamed('constructorId', 'constructor_id')
                               .withColumn('ingestion_date', current_timestamp())
                   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

qualifying_trans_df.write.mode('overwrite').parquet('dbfs:/mnt/formula1dlepam/processed/qualify')

# COMMAND ----------

display(qualifying_trans_df)
