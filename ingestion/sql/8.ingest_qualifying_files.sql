-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from lap_times directory 

-- COMMAND ----------

-- selecting the required columns and defining the ingestion date column
CREATE OR REPLACE TEMP VIEW qualifying_tv
USING JSON
OPTIONS(path '/mnt/formula1dlepam/raw/qualifying', multiline True)

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW qualifying_transformed 
AS
SELECT CAST(qualifyId AS INT) AS qualify_id
     , CAST(raceId AS INT) AS race_id
     , CAST(driverId AS INT) AS driver_id
     , CAST(constructorId AS INT) AS constructor_id
     , CAST(number AS INT) AS number
     , CAST(position AS INT) AS position
     , q1
     , q2
     , q3
     , CURRENT_TIMESTAMP() AS ingestion_date
FROM qualifying_tv

-- COMMAND ----------

-- creating the external table
USE processed;

CREATE TABLE IF NOT EXISTS qualifying
(qualify_id INT, race_id INT, driver_id INT, constructor_id INT, number INT, position INT, q1 STRING, q2 STRING, q3 STRING, ingestion_date TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:/mnt/formula1dlepam/processed/processed.db/qualifying'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

INSERT OVERWRITE qualifying
SELECT * FROM qualifying_transformed

-- COMMAND ----------

SELECT * FROM qualifying
