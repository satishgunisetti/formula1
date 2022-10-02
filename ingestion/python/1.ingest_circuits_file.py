# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting circuits.csv file from Mounted ADLS container  

# COMMAND ----------

# MAGIC %md
# MAGIC Checking the file path with below command  </br>
# MAGIC dbutils.fs.ls('/mnt/formula1dlepam/raw')

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, IntegerType, StringType

# COMMAND ----------

schema = StructType([StructField('circuit_id', StringType(), False),
                     StructField('circuit_ref', StringType(), True),
                     StructField('name', StringType(), True),
                     StructField('location', StringType(), True),
                     StructField('country', StringType(), True),
                     StructField('latitude', DoubleType(), True),
                     StructField('longitude', DoubleType(), True),
                     StructField('altitude', DoubleType(), True),
                     StructField('url', StringType(), True)
                    ]
                   )

# COMMAND ----------

circuits_df = ( spark.read
               .option('header', True)
               .schema(schema)
               .csv(f'dbfs:{raw_directory}/circuits.csv')
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the required columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = (circuits_df.select(col('circuit_id'), col('circuit_ref'), 
                                           col('name'), col('location'), col('country'), 
                                           col('latitude'), col('longitude'), col('altitude')))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Adding ingestion date column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet(f'dbfs:{processed_directory}/circuits')
