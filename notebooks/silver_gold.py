# Databricks notebook source
dbutils.widgets.text("yr_mnth", "2026-01")
yr_mnth = dbutils.widgets.get("yr_mnth")

# COMMAND ----------

# Storage account name
storage = "stnyctaxi01"

dim_location = spark.read.csv(f"abfss://bronze@{storage}.dfs.core.windows.net/taxi_zone_lookup.csv",header=True)
dim_location.write.mode("overwrite").csv(f"abfss://gold@{storage}.dfs.core.windows.net/dim_location")

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, hour

# Read cleaned data from Silver layer
df_clean = spark.read.option("header", "true").csv(f"abfss://silver@{storage}.dfs.core.windows.net/yellow_tripdata_{yr_mnth}")

# Create Date Dimension
dim_date = (
    df_clean
    .select("tpep_pickup_datetime")
    .distinct()
    .withColumn("year", year("tpep_pickup_datetime"))
    .withColumn("month", month("tpep_pickup_datetime"))
    .withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
    .withColumn("hour", hour("tpep_pickup_datetime"))
)

# Create Fact Trips
fact_trips = df_clean.select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type"
)

# Write Date Dimension to Gold layer
dim_date.write.mode("overwrite").option("header", "true").csv(f"abfss://gold@{storage}.dfs.core.windows.net/dim_date")

# Write Fact Trips to Gold layer
fact_trips.write.mode("overwrite").option("header", "true").csv(f"abfss://gold@{storage}.dfs.core.windows.net/fact_trips")