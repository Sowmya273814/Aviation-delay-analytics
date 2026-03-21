from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, lpad, concat_ws, to_date, mean, coalesce, lit, upper
from pyspark.sql.types import StringType, NumericType
from dotenv import load_dotenv
import os

# Load credentials
load_dotenv()
STORAGE_ACCOUNT = os.getenv("ACCOUNT_NAME")
STORAGE_KEY = os.getenv("ACCOUNT_KEY")
CONTAINER = os.getenv("CONTAINER_NAME")

BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/"

# 1. INITIALIZE SPARK SESSION
spark = SparkSession.builder \
    .appName("Optimizing-flight-aviation-Silver") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def standardize_and_clean(df):
    """Standardizes column names and removes duplicate rows."""
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower().strip())
    return df.dropDuplicates()

def intelligent_fill_nulls(df, label="dataset"):
    """Calculates means in one pass and fills nulls based on column context."""
    # Identify types based on existing schema
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    
    delay_reasons = {"air_system_delay", "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay"}
    cols_to_mean = [c for c in numeric_cols if c not in delay_reasons]
    
    mean_values = {}
    if cols_to_mean:
        print(f"[{label}] Calculating means for: {cols_to_mean}...")
        mean_row = df.select([mean(c).alias(c) for c in cols_to_mean]).collect()[0]
        mean_values = mean_row.asDict()

    for name in df.columns:
        if name in string_cols:
            df = df.withColumn(name, coalesce(trim(col(name)), lit("Unknown")))
        elif name in delay_reasons:
            # ANSI=false ensures malformed strings like '3.3.0' become NULL, then 0.0
            df = df.withColumn(name, coalesce(col(name).cast("double"), lit(0.0)))
        elif name in mean_values:
            val = mean_values[name] if mean_values[name] is not None else 0.0
            df = df.withColumn(name, coalesce(col(name).cast("double"), lit(val)))
    return df

# =========================
# 1. AIRLINES
# =========================
print("Processing AIRLINES...")
airlines = spark.read.option("header", True).csv(BRONZE_PATH + "airlines.csv")
airlines = standardize_and_clean(airlines)
airlines = intelligent_fill_nulls(airlines, "Airlines")
airlines = airlines.withColumn("airline", regexp_replace(col("airline"), r"\s+(Inc\.|Co\.|Airlines|Corp\.)", "")) \
                   .withColumn("iata_code", upper(col("iata_code")))
airlines.write.mode("overwrite").parquet(SILVER_PATH + "airlines")

# =========================
# 2. AIRPORTS
# =========================
print("Processing AIRPORTS...")
airports = spark.read.option("header", True).csv(BRONZE_PATH + "airports.csv")
airports = standardize_and_clean(airports)
airports = airports.withColumn("latitude", col("latitude").cast("double")) \
                   .withColumn("longitude", col("longitude").cast("double"))
airports = intelligent_fill_nulls(airports, "Airports")
airports = airports.withColumn("iata_code", upper(col("iata_code")))
airports.write.mode("overwrite").parquet(SILVER_PATH + "airports")

# =========================
# 3. FLIGHTS
# =========================
# =========================
# 3. FLIGHTS
# =========================
print("Processing FLIGHTS...")

# ✅ Load full dataset (ALL FILES)
flights = spark.read.option("header", True).csv(
    BRONZE_PATH + "flights_batch.csv"
)

print("Raw Flights Count:", flights.count())

flights = standardize_and_clean(flights)

# Cast numeric columns
numeric_cols_list = [
    "departure_delay", "arrival_delay", "distance",
    "air_system_delay", "security_delay",
    "airline_delay", "late_aircraft_delay", "weather_delay"
]

for c in numeric_cols_list:
    if c in flights.columns:
        flights = flights.withColumn(c, col(c).cast("double"))

flights = intelligent_fill_nulls(flights, "Flights")

# Fix date columns
date_parts = ["year", "month", "day"]
for part in date_parts:
    if part in flights.columns:
        flights = flights.withColumn(part, col(part).cast("double").cast("int"))

flights = flights.withColumn(
    "flight_date",
    to_date(concat_ws("-", col("year"), col("month"), col("day")))
)

# Format time + airport codes
flights = flights.withColumn(
    "scheduled_departure_time",
    regexp_replace(lpad(col("scheduled_departure"), 4, "0"), r"(\d{2})(\d{2})", "$1:$2")
).withColumn(
    "origin_airport", upper(col("origin_airport"))
).withColumn(
    "destination_airport", upper(col("destination_airport"))
)

# Remove invalid dates
flights = flights.filter(col("flight_date").isNotNull())

print("Final Flights Count:", flights.count())  # ✅ MUST BE ~5.8M

# Write to Silver
flights.write.mode("overwrite").parquet(SILVER_PATH + "flights")

print("Bronze → Silver pipeline completed successfully.")
spark.stop()