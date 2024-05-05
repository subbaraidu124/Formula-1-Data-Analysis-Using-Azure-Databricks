# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read files into dataframe

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

drivers_df = (spark.read.parquet(f"{processed_folder_path}/drivers")
.withColumnRenamed("number", "driver_number")
.withColumnRenamed("nationality", "driver_nationality"))

constructors_df = (spark.read.parquet(f"{processed_folder_path}/constructors") 
.withColumnRenamed("name", "team"))

circuits_df = (spark.read.parquet(f"{processed_folder_path}/circuits") 
.withColumnRenamed("location", "circuit_location"))

races_df = (spark.read.parquet(f"{processed_folder_path}/races") 
.withColumnRenamed("name", "race_name") 
.withColumnRenamed("race_timestamp", "race_date"))

results_df = (spark.read.parquet(f"{processed_folder_path}/results") 
.withColumnRenamed("time", "race_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create getDriverStandingsOfSeason function

# COMMAND ----------

def getDriverStandingsOfSeason(year):
    race_circuits_df = (races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")
        .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location))

    race_results_df = (results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id)
                                .join(drivers_df, results_df.driver_id == drivers_df.driverId)
                                .join(constructors_df, results_df.constructor_id == constructors_df.constructors_id))

    get_driver_rank = Window.orderBy(desc("points"))

    race_results_df = (race_results_df.filter(col("race_year") == year)
                       .groupBy("driver_name", "team")
                       .agg(sum("points").alias("points"), count(when(col("position") == 1, True)).alias("wins"))
                       .orderBy(desc("points"))
                      )

    display(race_results_df.withColumn("rank", rank().over(get_driver_rank))
                       .select("rank", "driver_name", "team", "wins", "points"))
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create getDriverStandings function

# COMMAND ----------

def getDriverStandings():
    race_circuits_df = (races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")
        .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location))

    race_results_df = (results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id)
                                .join(drivers_df, results_df.driver_id == drivers_df.driverId)
                                .join(constructors_df, results_df.constructor_id == constructors_df.constructors_id))

    get_driver_rank = Window.partitionBy("race_year").orderBy(desc("points"))

    race_results_df = (race_results_df
                       .groupBy("race_year","driver_name", "team")
                       .agg(sum("points").alias("points"), count(when(col("position") == 1, True)).alias("wins"))
                      )

    race_results_df = (race_results_df.withColumn("rank", rank().over(get_driver_rank))
                       .select("race_year", "driver_name", "rank", "team", "wins", "points")
                       .orderBy(desc("race_year"), desc("points")))
    return race_results_df
    

# COMMAND ----------

