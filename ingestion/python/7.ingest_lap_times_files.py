# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting from lap_times directory of Mounted ADLS container  

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

schema = StructType([StructField('race_id', IntegerType(), False),
                     StructField('driver_id', IntegerType(), True),
                     StructField('lap', IntegerType(), True),
                     StructField('position', IntegerType(), True),
                     StructField('time', StringType(), True),
                     StructField('milliseconds', IntegerType(), True)
                    ])

# COMMAND ----------

lap_times_df = ( spark.read.format('csv')
                    .option('path', '/mnt/formula1dlepam/raw/lap_times')
                    .schema(schema)
                    .load()
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and derive the required columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp

# COMMAND ----------

lap_times_trans_df = lap_times_df.withColumn('ingestion_date', current_timestamp())
                   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

lap_times_trans_df.write.mode('overwrite').parquet('dbfs:/mnt/formula1dlepam/processed/lap_times')
