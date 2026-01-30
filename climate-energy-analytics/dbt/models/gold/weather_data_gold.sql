{{ config(
  schema='gold',
  materialized='table',
  format='parquet',
  partitioned_by=['ingestion_date'],
  external_location='s3://climate-energy-raw-data/gold/weather_data/'
) }}


SELECT
  location,
  temperature_c,
  humidity,
  windspeed_mps,
  'openweather' as source,
  observation_date,
  ingestion_date
FROM {{ ref('openweather_silver') }}

UNION ALL

SELECT
  location,
  temperature_c,
  humidity,
  windspeed_mps,
  'openmeteo' as source,
  observation_date,
  ingestion_date
FROM {{ ref('openmeteo_silver') }}
