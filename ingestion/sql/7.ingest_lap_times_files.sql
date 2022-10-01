-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from lap_times directory 

-- COMMAND ----------

-- selecting the required columns and defining the ingestion date column
CREATE OR REPLACE TEMP VIEW lap_times_tv
(race_id INT, driver_id INT, lap INT, position INT, time STRING, milliseconds INT)
USING CSV 
OPTIONS(path '/mnt/formula1dlepam/raw/lap_times')

-- COMMAND ----------

SELECT * FROM lap_times_tv

-- COMMAND ----------

-- creating the external table

USE processed;
CREATE TABLE IF NOT EXISTS lap_times
(race_id INT, driver_id INT, lap INT, position INT, time STRING, milliseconds INT, ingestion_date TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:/mnt/formula1dlepam/processed/processed.db/lap_times'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

INSERT OVERWRITE lap_times
SELECT *, CURRENT_TIMESTAMP() AS ingestion_date  FROM lap_times_tv

-- COMMAND ----------

SELECT * FROM  lap_times
