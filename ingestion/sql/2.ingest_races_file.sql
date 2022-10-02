-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from races.csv 

-- COMMAND ----------

-- MAGIC %run "../../includes/configuration"

-- COMMAND ----------

-- creating the temp view with required column names 

CREATE OR REPLACE TEMP VIEW races_tv
(race_id INT, race_year INT, round INT, circuit_id INT, name STRING, date STRING, time STRING, url STRING)
USING CSV
OPTIONS (path = '${raw.directory}/races.csv', header = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Generate and fetch requited columns

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW races_transformed
AS
SELECT race_id
     , race_year
     , round
     , circuit_id
     , name
     , TO_TIMESTAMP(ARRAY_JOIN(ARRAY(date, time), ' '), 'yyyy-MM-dd HH:mm:ss') as race_timestamp
     , CURRENT_TIMESTAMP as ingestion_timestamp
FROM races_tv

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS processed.races (
  race_id INT,
  race_year INT,
  round INT,
  circuit_id INT,
  name STRING,
  race_timestamp TIMESTAMP,
  ingestion_timestamp TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:${processed.directory}/races'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------


INSERT OVERWRITE processed.races
SELECT * FROM races_transformed;

-- COMMAND ----------

SELECT * FROM  races;
