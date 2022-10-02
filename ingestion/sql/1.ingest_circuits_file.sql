-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using SQL we are loading the data from circuits.csv 

-- COMMAND ----------

-- MAGIC %run "../../includes/configuration"

-- COMMAND ----------

-- creating the temp view with required column names 

CREATE OR REPLACE TEMP VIEW circuits_tv
(circuit_id STRING, circuit_ref STRING, name STRING, location STRING, country STRING, latitude DOUBLE, longitude DOUBLE, altitude DOUBLE, url STRING)
USING CSV
OPTIONS (path="${raw.directory}/circuits.csv", header="true")

-- COMMAND ----------

-- selecting the required columns and defining the ingestion date column

CREATE OR REPLACE TEMP VIEW circuits_selected
AS
  SELECT circuit_id
       , circuit_ref
       , name
       , location
       , country
       , latitude
       , longitude
       , altitude
       , CURRENT_TIMESTAMP() AS ingestion_date
  FROM circuits_tv


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS processed LOCATION '${processed.directory}';

-- COMMAND ----------

USE processed;

CREATE TABLE IF NOT EXISTS processed.circuits (
  circuit_id STRING,
  circuit_ref STRING,
  name STRING,
  location STRING,
  country STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  altitude DOUBLE,
  ingestion_date TIMESTAMP)
USING PARQUET
LOCATION 'dbfs:${processed.directory}/circuits'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write data to datalake as parquet file

-- COMMAND ----------

INSERT OVERWRITE processed.circuits 
SELECT * FROM circuits_selected;

-- COMMAND ----------

SELECT * FROM  circuits
