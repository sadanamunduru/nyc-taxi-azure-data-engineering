# Databricks notebook source
dbutils.widgets.text("yr_mnth", "")
yr_mnth = dbutils.widgets.get("yr_mnth")

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp

# Storage account name
storage = "stnyctaxi01"

# Read data from Bronze layer
df = spark.read.parquet(
    f"abfss://bronze@{storage}.dfs.core.windows.net/yellow_tripdata_{yr_mnth}.parquet"
)

# Clean the data
df_clean = (
    df
    # Remove records with invalid passenger count
    .filter(col("passenger_count") > 0)

    # Remove records with invalid trip distance
    .filter(col("trip_distance") > 0)

    # Remove records with invalid fare amount
    .filter(col("fare_amount") > 0)

    # Calculate trip duration in minutes
    .withColumn(
        "trip_duration_min",
        (
            unix_timestamp("tpep_dropoff_datetime")
            - unix_timestamp("tpep_pickup_datetime")
        ) / 60
    )

    # Remove records with invalid trip duration
    .filter(col("trip_duration_min") > 0)

    # Remove duplicate records
    .dropDuplicates()
)

# Write cleaned data to Silver layer
df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"abfss://silver@{storage}.dfs.core.windows.net/yellow_tripdata_{yr_mnth}")