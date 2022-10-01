-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from drivers.json 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW drivers_tv
AS
SELECT * FROM JSON.`/mnt/formula1dlepam/raw/drivers.json`

-- COMMAND ----------

-- selecting the required columns and defining the ingestion date column
CREATE OR REPLACE TEMP VIEW drivers_formatted
AS
SELECT CAST(driverId AS INT) AS driver_id
     , driverRef AS driver_ref
     , CAST(number AS INT) AS number
     , code
     , ARRAY_JOIN(ARRAY(name.forename, name.surname), ' ') AS name
     , CAST(dob AS DATE) AS dob
     , nationality
     , CURRENT_DATE() AS ingestion_date
FROM drivers_tv

-- COMMAND ----------

-- creating the external table
USE processed;

CREATE TABLE IF NOT EXISTS drivers
(driver_id INT, driver_ref STRING, number INT, code STRING, name STRING, dob DATE, nationality STRING, ingestion_date TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:/mnt/formula1dlepam/processed/processed.db/drivers'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

INSERT OVERWRITE drivers
SELECT * FROM drivers_formatted

-- COMMAND ----------

SELECT * FROM drivers
