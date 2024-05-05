# Databricks notebook source
# MAGIC %run "./user_defined_functions"

# COMMAND ----------

display(getDriverStandingsOfSeason(2019))
# race_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

driver_standings_df = getDriverStandings()
driver_standings_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

spark.sql("USE f1_presentation")
driver_standings_df.write.mode("overwrite").format("delta").saveAsTable("driver_standings")

# COMMAND ----------

