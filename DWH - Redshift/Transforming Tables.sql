
INSERT INTO dim_soil (ph, nitrogen, phosphorus, potassium)
SELECT DISTINCT
  ph, nitrogen, phosphorus, potassium
FROM valid_readings
WHERE ph IS NOT NULL
  AND nitrogen IS NOT NULL
  AND phosphorus IS NOT NULL
  AND potassium IS NOT NULL;

INSERT INTO dim_time (full_date, year, month, day, hour, minute)
SELECT DISTINCT
  timestamp AS full_date,
  EXTRACT(YEAR FROM timestamp),
  EXTRACT(MONTH FROM timestamp),
  EXTRACT(DAY FROM timestamp),
  EXTRACT(HOUR FROM timestamp),
  EXTRACT(MINUTE FROM timestamp)
FROM valid_readings
WHERE timestamp IS NOT NULL;

INSERT INTO dim_location (loc_id, latitude, longitude)
SELECT DISTINCT
  loc_id, latitude, longitude
FROM valid_readings
WHERE loc_id IS NOT NULL;


INSERT INTO dim_weather (
  weather_temperature,
  weather_humidity,
  wind_speed,
  wind_direction,
  rain,
  surface_pressure
)
SELECT DISTINCT
  weather_temperature_2m,
  weather_relative_humidity_2m,
  weather_wind_speed_10m,
  weather_wind_direction_10m,
  weather_rain,
  weather_surface_pressure
FROM valid_readings
WHERE weather_temperature_2m IS NOT NULL;


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

  -- Location key
  l.location_key,

  -- Weather key
  w.weather_key,

  -- Soil key
  s.soil_key,

  -- Time key
  t.full_date,

  -- Measures
  v.temperature AS soil_temperature,
  v.humidity AS soil_humidity,
  v.water_level,
  v.validation_status

FROM valid_readings v

JOIN dim_location l
  ON v.loc_id = l.loc_id
  AND v.latitude = l.latitude
  AND v.longitude = l.longitude

JOIN dim_weather w
  ON v.weather_temperature_2m = w.weather_temperature
  AND v.weather_relative_humidity_2m = w.weather_humidity
  AND v.weather_wind_speed_10m = w.wind_speed
  AND v.weather_wind_direction_10m = w.wind_direction
  AND v.weather_rain = w.rain
  AND v.weather_surface_pressure = w.surface_pressure

JOIN dim_soil s
  ON v.ph = s.ph
  AND v.nitrogen = s.nitrogen
  AND v.phosphorus = s.phosphorus
  AND v.potassium = s.potassium

JOIN dim_time t
  ON v.timestamp = t.full_date;
