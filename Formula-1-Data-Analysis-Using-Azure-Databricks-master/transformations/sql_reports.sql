-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Find Dominant Drivers
-- MAGIC  ##### 1) Drivers and their race wins
-- MAGIC  ##### 2) Drivers and their total championships

-- COMMAND ----------

USE f1_presentation;
SELECT * FROM driver_standings;

-- COMMAND ----------

CREATE OR REPLACE TABLE drivers_wins
AS
SELECT driver_name, sum(wins) AS total_wins from driver_standings where wins > 0
GROUP BY driver_name
ORDER BY total_wins desc;

SELECT * FROM drivers_wins;

-- COMMAND ----------

CREATE OR REPLACE TABLE champions_list
AS
SELECT driver_name, count(rank) AS total_championships from driver_standings where rank = 1
GROUP BY driver_name
ORDER BY total_championships desc;

SELECT * FROM champions_list;

-- COMMAND ----------

