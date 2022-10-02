# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting constructors.json file from Mounted ADLS container  

# COMMAND ----------

# MAGIC %fs
# MAGIC head /mnt/formula1dlepam/raw/constructors.json

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

cons_df = ( spark.read.format('json')
               .schema(schema)
               .load(f'{raw_directory}/constructors.json')
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and derive the required columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

cons_transformed_df = ( cons_df.withColumnRenamed('constructorId', 'constructor_id')
                               .withColumnRenamed('constructorRef', 'constructor_ref')
                               
                      )
cons_final_df = add_ingestion_date(cons_transformed_df).select('constructor_id', 'constructor_ref', 'name', 'nationality', 'ingestion_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

cons_final_df.write.mode('overwrite').parquet(f'dbfs:{processed_directory}/constructors')
