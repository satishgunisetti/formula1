# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting races.csv file from Mounted ADLS container  

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

schema = StructType([StructField('race_id', IntegerType(), False),
                     StructField('race_year', IntegerType(), True),
                     StructField('round', IntegerType(), True),
                     StructField('circuit_id', IntegerType(), True),
                     StructField('name', StringType(), True),
                     StructField('date', StringType(), True),
                     StructField('time', StringType(), True),
                     StructField('url', StringType(), True)
                    ])

# COMMAND ----------

races_df = (spark.read
            .format('csv')
            .option('path', '/mnt/formula1dlepam/raw/races.csv')
            .option('header', True)
            .schema(schema)
            .load())
display(races_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### generating and selecting required columns

# COMMAND ----------

from pyspark.sql.functions import col, array_join, array, to_timestamp, current_timestamp

# COMMAND ----------

races_trans_df = ( races_df.withColumn('race_timestamp',  to_timestamp(array_join(array(col('date'), col('time')), ' '), 'yyyy-MM-dd HH:mm:ss'))
                          .withColumn('ingestion_date', current_timestamp()) 
                )
races_select_df = races_trans_df.select('race_id', 'race_year', 'round', 'circuit_id', 'name', 'race_timestamp', 'ingestion_date')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

races_select_df.write.mode('overwrite').parquet('/mnt/formula1dlepam/processed/races')
