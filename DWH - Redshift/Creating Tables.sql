CREATE TABLE dim_soil (
  soil_key INT IDENTITY(1,1) PRIMARY KEY,
  ph DOUBLE PRECISION,
  nitrogen DOUBLE PRECISION,
  phosphorus DOUBLE PRECISION,
  potassium DOUBLE PRECISION
);

CREATE TABLE dim_time (
  full_date TIMESTAMP PRIMARY KEY,
  year INT,
  month INT,
  day INT,
  hour INT,
  minute INT
);

CREATE TABLE dim_location (
  location_key INT IDENTITY(1,1) PRIMARY KEY,
  loc_id VARCHAR(50),
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION
);


CREATE TABLE dim_weather (
  weather_key INT IDENTITY(1,1) PRIMARY KEY,
  weather_temperature DOUBLE PRECISION,
  weather_humidity DOUBLE PRECISION,
  wind_speed DOUBLE PRECISION,
  wind_direction DOUBLE PRECISION,
  rain DOUBLE PRECISION,
  surface_pressure DOUBLE PRECISION
);

CREATE TABLE fact_sensor_readings (
  fact_id INT IDENTITY(1,1) PRIMARY KEY,

  evt_id VARCHAR(100),

  location_key INT REFERENCES dim_location(location_key),
  weather_key INT REFERENCES dim_weather(weather_key),
  soil_key INT REFERENCES dim_soil(soil_key),
  full_date TIMESTAMP REFERENCES dim_time(full_date),

  soil_temperature DOUBLE PRECISION,
  soil_humidity DOUBLE PRECISION,
  water_level DOUBLE PRECISION,

  validation_status VARCHAR(20)
);
