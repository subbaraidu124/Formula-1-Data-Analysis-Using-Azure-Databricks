# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingested_date(input_df):
    output_df = input_df.withColumn("ingested_date", current_timestamp())
    return output_df

# COMMAND ----------

