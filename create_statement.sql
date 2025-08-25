-- Ejecutar en AWS Athena
CREATE EXTERNAL TABLE IF NOT EXISTS weather_cleaned (
  timestamp string,
  city_name string,
  lat double,
  lon double,
  temp double,
  feels_like double,
  pressure int,
  humidity int,
  speed double,
  deg int,
  cloudiness int
)
STORED AS PARQUET
LOCATION 's3://pi-m3-silver/clima/cleaned/';
