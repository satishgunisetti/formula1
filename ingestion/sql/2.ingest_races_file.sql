-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from races.csv 

-- COMMAND ----------

-- creating the temp view with required column names 

CREATE OR REPLACE TEMP VIEW races_tv
(race_id INT, race_year INT, round INT, circuit_id INT, name STRING, date STRING, time STRING, url STRING)
USING CSV
OPTIONS (path = '/mnt/formula1dlepam/raw/races.csv', header = 'true')

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

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

USE processed;

CREATE TABLE races 
USING PARQUET LOCATION 'dbfs:/mnt/formula1dlepam/processed/processed.db/races'
AS
SELECT * FROM races_transformed;

-- COMMAND ----------

SELECT * FROM races;
