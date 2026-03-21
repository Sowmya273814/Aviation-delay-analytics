import snowflake.connector
from dotenv import load_dotenv
import os

# Load Environment Variables for Security
load_dotenv()

# Snowflake Connection Details
# Pro-tip: Move these to your .env file for security!
SF_USER = os.getenv("SF_USER", "SOWMYA273814")
SF_PASS = os.getenv("SF_PASS", "Sowmyayashwanth27")
SF_ACCOUNT = os.getenv("SF_ACCOUNT", "DOEDSPI-IG33948")

conn = snowflake.connector.connect(
    user=SF_USER,
    password=SF_PASS,
    account=SF_ACCOUNT,
    warehouse='COMPUTE_WH',
    database='AVIATION_DB',
    schema='PUBLIC',
    role='ACCOUNTADMIN'
)

try:
    cur = conn.cursor()

    # -------------------------------
    # 1. Create External Stage (Azure)
    # -------------------------------
    # Using the SAS token provided for the gold container
    cur.execute(f"""
    CREATE OR REPLACE STAGE gold_azure_stage
    URL = 'azure://flightaviation.blob.core.windows.net/sowmya/gold/'
    CREDENTIALS = (
        AZURE_SAS_TOKEN = '?sp=rwl&st=2026-03-17T08:39:31Z&se=2026-04-04T16:54:31Z&spr=https&sv=2024-11-04&sr=c&sig=Lzg066sEmZS19avCCWVuNZKw6LkYEcD8yxuyfqLxy34%3D'
    )
    FILE_FORMAT = (TYPE = 'PARQUET');
    """)
    print("✅ Azure Gold Stage created.")

    # -------------------------------
    # 2. Define Tables (Updated Schema)
    # -------------------------------

    # DIM_AIRLINES
    cur.execute("""
    CREATE OR REPLACE TABLE dim_airlines (
    airline_id STRING,
    airline_name STRING
    );
    """)

    # DIM_AIRPORTS
    cur.execute("""
    CREATE OR REPLACE TABLE dim_airports (
    airport_id STRING,
    airport_name STRING,
    city STRING,
    state STRING,
    latitude DOUBLE,
    longitude DOUBLE
    );
    """)

    # DIM_DATE
    cur.execute("""
    CREATE OR REPLACE TABLE dim_date (
    flight_date DATE,
    year INT,
    month INT,
    day INT,
    day_of_week INT
    );
    """)

    # FACT_FLIGHTS
    cur.execute("""
    CREATE OR REPLACE TABLE fact_flights (
    flight_date DATE,
    airline_id STRING,
    origin_airport_id STRING,
    dest_airport_id STRING,
    departure_delay DOUBLE,
    arrival_delay DOUBLE,
    distance DOUBLE,
    air_time DOUBLE,
    cancelled INT,
    diverted INT,
    is_delayed INT,
    delay_minutes DOUBLE,
    delay_category STRING
    );
    """)

    # ✅ MAIN ANALYTICS TABLE (VERY IMPORTANT)
    cur.execute("""
    CREATE OR REPLACE TABLE flight_analytics (
    flight_date DATE,
    airline_id STRING,
    airline_name STRING,
    origin_airport_id STRING,
    airport_name STRING,
    city STRING,
    state STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    departure_delay DOUBLE,
    arrival_delay DOUBLE,
    distance DOUBLE,
    air_time DOUBLE,
    cancelled INT,
    diverted INT,
    is_delayed INT,
    delay_minutes DOUBLE,
    delay_category STRING
    );
    """)

    print("✅ Tables initialized in Snowflake.")

    # -------------------------------
    # 3. Load Data using COPY INTO
    # -------------------------------

    load_mapping = {
        "dim_airlines": "dim_airlines",
        "dim_airports": "dim_airports",
        "dim_date": "dim_date",
        "fact_flights": "fact_flights",
        "flight_analytics": "flight_analytics"   # ✅ NEW
    }

    for folder, table in load_mapping.items():
        print(f"Loading {table}...")

        # Optional: Clear existing data
        cur.execute(f"TRUNCATE TABLE {table}")

        cur.execute(f"""
        COPY INTO {table}
        FROM @gold_azure_stage/{folder}/
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
        """)

        # Validate count
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]

        print(f"✅ {table} loaded successfully with {count} rows")

    print("🎯 All tables loaded successfully!")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    # -------------------------------
    # CLEANUP
    # -------------------------------

    cur.close()
    conn.close()

    print("✅ Snowflake connection closed.")


