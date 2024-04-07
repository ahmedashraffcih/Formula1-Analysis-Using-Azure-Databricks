-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
using parquet
AS
SELECT 
  rc.race_year,
  c.name team_name,
  d.name driver_name,
  r.position,
  r.points,
  11 - r.position calculated_points
FROM f1_processed.results r 
JOIN f1_processed.drivers d using(driver_id)
JOIN f1_processed.constructors c using(constructor_id)
JOIN f1_processed.races rc using(race_id)
WHERE r.position <=10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------


