-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from drivers.json 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW results_tv
AS
SELECT * FROM JSON.`/mnt/formula1dlepam/raw/results.json`

-- COMMAND ----------

SELECT * FROM results_tv

-- COMMAND ----------

-- selecting the required columns and defining the ingestion date column
CREATE OR REPLACE TEMP VIEW results_transformed
AS
SELECT CAST(resultId AS INT) AS result_id 
     , CAST(raceId AS INT) AS race_id
     , CAST(driverId AS INT) AS driver_id
     , CAST(constructorId AS INT) AS constructor_id
     , CAST(number AS INT) AS number
     , CAST(grid AS INT) AS grid
     , CAST(position AS INT) AS position
     , positionText AS position_text
     , CAST(positionOrder AS INT) AS position_order
     , CAST(points AS DOUBLE) AS points
     , CAST(laps AS INT) AS laps
     , time
     , CAST(milliseconds AS INT) AS milliseconds
     , CAST(fastestLap AS INT) AS fastest_lap
     , CAST(rank AS INT) AS rank
     , fastestLapTime AS fastest_lap_time
     , fastestLapSpeed AS fastest_lap_field
     , CURRENT_TIMESTAMP() AS ingestion_date
FROM results_tv

-- COMMAND ----------

-- creating the external table
USE processed;

CREATE TABLE IF NOT EXISTS results
(result_id INT, race_id INT, driver_id INT, constructor_id INT, number INT, grid INT, position INT, position_text STRING, position_order INT, points DOUBLE, laps INT, time STRING, 
 milliseconds INT, fastest_lap INT, rank INT, fastest_lap_time STRING, fastest_lap_field STRING, ingestion_date TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:/mnt/formula1dlepam/processed/processed.db/results'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

INSERT OVERWRITE results
SELECT * FROM results_transformed

-- COMMAND ----------

SELECT * FROM results
