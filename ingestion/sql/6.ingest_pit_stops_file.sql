-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from pit_stops.json 

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head /mnt/formula1dlepam/raw/pit_stops.json

-- COMMAND ----------

-- MAGIC %run "../../includes/configuration"

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW pit_stops_tv
USING JSON OPTIONS(path '${raw.directory}/pit_stops.json', multiline True)

-- COMMAND ----------

-- selecting the required columns and defining the ingestion date column
CREATE OR REPLACE TEMP VIEW pit_stops_transformed
AS
SELECT CAST(raceId AS INT) AS race_id
     , CAST(driverId AS INT) AS driver_id
     , stop
     , CAST(lap AS INT) AS lap
     , time
     , duration
     , CAST(milliseconds AS INT) AS milliseconds
     , CURRENT_TIMESTAMP() AS ingestion_date
FROM pit_stops_tv

-- COMMAND ----------

-- creating the external table
USE processed;

CREATE TABLE IF NOT EXISTS pit_stops
(race_id INT, driver_id INT, stop STRING, lap INT, time STRING, duration STRING, milliseconds INT, ingestion_date TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:${processed.directory}/pit_stops'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

INSERT OVERWRITE pit_stops
SELECT * FROM pit_stops_transformed

-- COMMAND ----------

SELECT * FROM pit_stops
