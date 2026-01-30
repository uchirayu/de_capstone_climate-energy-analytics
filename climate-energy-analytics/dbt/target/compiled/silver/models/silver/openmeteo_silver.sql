

SELECT
  city as location,
  temperature as temperature_c,
  temperature * (9.0 / 5.0) + 32.0 as temperature_f,
  temperature_min,
  temperature_min * (9.0 / 5.0) + 32.0 as temperature_min_f,
  precipitation_mm as precipitation,
  LEAST(100, precipitation_mm * 10) as humidity,
  windspeed as windspeed_mps,
  windspeed * 2.23694 as windspeed_mph,
  CAST(date_str AS date) AS dt_timestamp,
  CAST(date_str AS date) AS observation_date,
  CAST(from_iso8601_timestamp(ingestion_timestamp) AS date) AS ingestion_date
FROM "AwsDataCatalog"."bronze"."openmeteo_bronze"