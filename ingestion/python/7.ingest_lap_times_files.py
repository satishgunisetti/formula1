# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting from lap_times directory of Mounted ADLS container  

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

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
                    .option('path', f'{raw_directory}/lap_times')
                    .schema(schema)
                    .load()
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and derive the required columns from the dataframe

# COMMAND ----------

lap_times_trans_df = add_ingestion_date(lap_times_df)                   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

lap_times_trans_df.write.mode('overwrite').parquet(f'dbfs:{processed_directory}/lap_times')
