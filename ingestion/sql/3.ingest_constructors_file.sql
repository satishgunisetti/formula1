-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from circuits.json 

-- COMMAND ----------

-- MAGIC %run "../../includes/configuration"

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW constructors_tv AS

SELECT * FROM JSON.`${raw.directory}/constructors.json`

-- COMMAND ----------

-- selecting the required columns and defining the ingestion date column

CREATE OR REPLACE TEMP VIEW constructors_transformed AS
SELECT CAST(constructorId AS INT)  AS constructor_id
     , constructorRef AS constructor_ref
     , name
     , nationality
     , CURRENT_TIMESTAMP() AS ingestion_date
FROM constructors_tv

-- COMMAND ----------

-- creating the external table
USE processed;

CREATE TABLE IF NOT EXISTS constructors
(constructor_id INT, constructor_ref STRING, name STRING, nationality STRING, ingestion_date TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:${processed.directory}/constructors'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

INSERT OVERWRITE constructors
SELECT * FROM constructors_transformed

-- COMMAND ----------

REFRESH TABLE constructors

-- COMMAND ----------

SELECT * FROM constructors;
