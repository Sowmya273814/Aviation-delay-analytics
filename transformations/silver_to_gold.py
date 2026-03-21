from utils.spark_session import create_spark_session
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, when
from dotenv import load_dotenv
import os

print("Starting Silver → Gold Pipeline")

# =========================

# LOAD ENV VARIABLES

# =========================

load_dotenv()

STORAGE_ACCOUNT = os.getenv("ACCOUNT_NAME")
STORAGE_KEY = os.getenv("ACCOUNT_KEY")
CONTAINER = os.getenv("CONTAINER_NAME")

SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/"
GOLD_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"

# =========================

# CREATE SPARK SESSION

# =========================

spark = create_spark_session("SilverToGold")

spark.conf.set(
f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
STORAGE_KEY
)

# =========================

# READ SILVER DATA

# =========================

print("Reading Silver Layer Data")

flights = spark.read.parquet(SILVER_PATH + "flights")
airlines = spark.read.parquet(SILVER_PATH + "airlines")
airports = spark.read.parquet(SILVER_PATH + "airports")

print("Flights Count:", flights.count())  # DEBUG

# =========================

# DIMENSION TABLES

# =========================

print("Creating Dimension Tables")

dim_airlines = airlines.select(
col("iata_code").alias("airline_id"),
col("airline").alias("airline_name")
).dropDuplicates(["airline_id"])

dim_airports = airports.select(
col("iata_code").alias("airport_id"),
col("airport").alias("airport_name"),
col("city"),
col("state"),
col("latitude"),
col("longitude")
).dropDuplicates(["airport_id"])

dim_date = flights.select("flight_date").distinct() \
.withColumn("year", year(col("flight_date"))) \
.withColumn("month", month(col("flight_date"))) \
.withColumn("day", dayofmonth(col("flight_date"))) \
.withColumn("day_of_week", dayofweek(col("flight_date"))) \
.dropna()

# =========================

# FACT TABLE (ENHANCED)

# =========================

print("Creating Fact Table")

fact_flights = flights.select(
col("flight_date"),
col("airline").alias("airline_id"),
col("origin_airport").alias("origin_airport_id"),
col("destination_airport").alias("dest_airport_id"),
col("departure_delay"),
col("arrival_delay"),
col("distance"),
col("air_time"),
col("cancelled"),
col("diverted")
)

# =========================

# ADD ANALYTICS COLUMNS

# =========================

print("Adding KPI Columns")

fact_flights = fact_flights.withColumn(
"is_delayed",
when(col("arrival_delay") > 0, 1).otherwise(0)
).withColumn(
"delay_minutes",
when(col("arrival_delay") > 0, col("arrival_delay")).otherwise(0)
).withColumn(
"delay_category",
when(col("arrival_delay") <= 0, "On Time")
.when(col("arrival_delay") <= 15, "Minor Delay")
.when(col("arrival_delay") <= 60, "Moderate Delay")
.otherwise("Severe Delay")
)

# =========================

# CREATE ANALYTICS TABLE (MAIN)

# =========================

print("Creating Flight Analytics Table")

flight_analytics = fact_flights.join(
dim_airlines, "airline_id", "left"
).join(
dim_airports.withColumnRenamed("airport_id", "origin_airport_id"),
"origin_airport_id",
"left"
).join(
dim_date, "flight_date", "left"
)

# =========================

# WRITE GOLD LAYER

# =========================

print("Writing Gold Layer")

dim_airlines.write.mode("overwrite").parquet(GOLD_PATH + "dim_airlines")
dim_airports.write.mode("overwrite").parquet(GOLD_PATH + "dim_airports")
dim_date.write.mode("overwrite").parquet(GOLD_PATH + "dim_date")
fact_flights.write.mode("overwrite").parquet(GOLD_PATH + "fact_flights")

# MAIN ANALYTICS TABLE (USE THIS IN SNOWFLAKE + DASHBOARD)

flight_analytics.write.mode("overwrite").parquet(GOLD_PATH + "flight_analytics")

print("Gold Layer Created Successfully")

spark.stop()
