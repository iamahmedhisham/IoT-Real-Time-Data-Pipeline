import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# Initialize Spark & Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Redshift connection details
redshift_jdbc_url = "jdbc:redshift://workgroup-01.311141522446.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
redshift_user = "admin"
redshift_password = "de2025_ITI"

# -----------------------------------------------------------------------------
# STEP 1: Identify new data from valid_readings to load into the warehouse
# -----------------------------------------------------------------------------
print("Identifying new data from 'valid_readings' to load into Dimension and Fact tables...")

# Get the last loaded timestamp from the target fact table to ensure incremental load
try:
    last_ts_df = spark.read \
        .format("jdbc") \
        .option("url", redshift_jdbc_url) \
        .option("query", "SELECT MAX(full_date) AS last_ts FROM fact_sensor_readings") \
        .option("user", redshift_user) \
        .option("password", redshift_password) \
        .load()
    last_ts = last_ts_df.collect()[0]['last_ts']
    if last_ts is None:
        # If the fact table is empty, start from a very old timestamp
        last_ts = '1970-01-01 00:00:00.000'
    print(f"Last loaded timestamp from Redshift Fact table: {last_ts}")
except Exception as e:
    print(f"Could not read last timestamp from Redshift Fact table: {e}. Assuming first run.")
    last_ts = '1970-01-01 00:00:00.000'

# Read all data from the source table (valid_readings)
valid_readings_df = spark.read \
    .format("jdbc") \
    .option("url", redshift_jdbc_url) \
    .option("dbtable", "public.valid_readings") \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .load()

# Filter for records newer than the last loaded timestamp
new_data_df = valid_readings_df.filter(col("timestamp") > last_ts)

if new_data_df.rdd.isEmpty():
    print("No new data to load. The job will now complete.")
    # We no longer use sys.exit() to allow the job to finish gracefully
else:
    print(f"Processing {new_data_df.count()} new records.")
    # Create a temporary view from the new data
    new_data_df.createOrReplaceTempView("new_valid_readings")

    # -----------------------------------------------------------------------------
    # STEP 2: Load into dimensions and fact table from the staging area
    # -----------------------------------------------------------------------------
    print("Starting load from new data to Dimension and Fact tables...")

    # Function to execute a single SQL query
    def execute_sql_query(query_string):
        try:
            spark.sql(query_string)
            print(f"Query executed successfully.")
        except Exception as e:
            print(f"Error executing query: {e}")

    # Insert into Dimension tables
    print("Loading data into Dimension tables...")

    # dim_location: Insert only new, unique records
    execute_sql_query("""
        INSERT INTO dim_location (loc_id, latitude, longitude)
        SELECT DISTINCT nv.loc_id, nv.latitude, nv.longitude
        FROM new_valid_readings nv
        WHERE nv.loc_id IS NOT NULL
          AND nv.loc_id NOT IN (SELECT loc_id FROM dim_location)
    """)

    # dim_time: Insert only new, unique records
    execute_sql_query("""
        INSERT INTO dim_time (full_date, year, month, day, hour, minute)
        SELECT DISTINCT
            nv.timestamp AS full_date,
            EXTRACT(YEAR FROM nv.timestamp),
            EXTRACT(MONTH FROM nv.timestamp),
            EXTRACT(DAY FROM nv.timestamp),
            EXTRACT(HOUR FROM nv.timestamp),
            EXTRACT(MINUTE FROM nv.timestamp)
        FROM new_valid_readings nv
        WHERE nv.timestamp IS NOT NULL
          AND nv.timestamp NOT IN (SELECT full_date FROM dim_time)
    """)

    # dim_soil: Insert only new, unique records using NOT EXISTS for efficiency
    execute_sql_query("""
        INSERT INTO dim_soil (ph, nitrogen, phosphorus, potassium)
        SELECT DISTINCT nv.ph, nv.nitrogen, nv.phosphorus, nv.potassium
        FROM new_valid_readings nv
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_soil ds
            WHERE ds.ph = nv.ph
              AND ds.nitrogen = nv.nitrogen
              AND ds.phosphorus = nv.phosphorus
              AND ds.potassium = nv.potassium
        )
        AND nv.ph IS NOT NULL
        AND nv.nitrogen IS NOT NULL
        AND nv.phosphorus IS NOT NULL
        AND nv.potassium IS NOT NULL
    """)

    # dim_weather: Insert only new, unique records using NOT EXISTS for efficiency
    execute_sql_query("""
        INSERT INTO dim_weather (
            weather_temperature,
            weather_humidity,
            wind_speed,
            wind_direction,
            rain,
            surface_pressure)
        SELECT DISTINCT
            nv.weather_temperature_2m,
            nv.weather_relative_humidity_2m,
            nv.weather_wind_speed_10m,
            nv.weather_wind_direction_10m,
            nv.weather_rain,
            nv.weather_surface_pressure
        FROM new_valid_readings nv
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_weather dw
            WHERE dw.weather_temperature = nv.weather_temperature_2m
              AND dw.weather_humidity = nv.weather_relative_humidity_2m
              AND dw.wind_speed = nv.weather_wind_speed_10m
              AND dw.wind_direction = nv.weather_wind_direction_10m
              AND dw.rain = nv.weather_rain
              AND dw.surface_pressure = nv.weather_surface_pressure
        )
        AND nv.weather_temperature_2m IS NOT NULL
    """)

    # Insert into Fact table
    print("Loading data into Fact table...")
    # This query joins the NEW data with the updated dimension tables to get the correct keys.
    execute_sql_query("""
        INSERT INTO fact_sensor_readings (
            evt_id,
            location_key,
            weather_key,
            soil_key,
            full_date,
            soil_temperature,
            soil_humidity,
            water_level,
            validation_status
        )
        SELECT
            v.event_id,
            l.location_key,
            w.weather_key,
            s.soil_key,
            t.full_date,
            v.temperature AS soil_temperature,
            v.humidity AS soil_humidity,
            v.water_level,
            v.validation_status
        FROM new_valid_readings v
        JOIN dim_location l
            ON v.loc_id = l.loc_id AND v.latitude = l.latitude AND v.longitude = l.longitude
        JOIN dim_weather w
            ON v.weather_temperature_2m = w.weather_temperature
            AND v.weather_relative_humidity_2m = w.weather_humidity
            AND v.weather_wind_speed_10m = w.wind_speed
            AND v.weather_wind_direction_10m = w.wind_direction
            AND v.weather_rain = w.rain
            AND v.weather_surface_pressure = w.surface_pressure
        JOIN dim_soil s
            ON v.ph = s.ph AND v.nitrogen = s.nitrogen
            AND v.phosphorus = s.phosphorus AND v.potassium = s.potassium
        JOIN dim_time t
            ON v.timestamp = t.full_date
    """)
