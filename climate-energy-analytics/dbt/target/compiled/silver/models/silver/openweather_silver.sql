

SELECT
  city as location,
  temperature as temperature_c,
  temperature * (9.0 / 5.0) + 32.0 as temperature_f,
  feelslike_temp,
  feelslike_temp * (9.0 / 5.0) + 32.0 as feelslike_temp_f,
  humidity,
  windspeed as windspeed_mps,
  windspeed * 2.23694 as windspeed_mph,
  visibility,
  weather_status,
  from_unixtime(epoch_ts) AS dt_timestamp,
  CAST(from_iso8601_timestamp(ingestion_timestamp) AS date) AS ingestion_date,
  CAST(from_unixtime(epoch_ts) AS date) AS observation_date
FROM "AwsDataCatalog"."bronze"."openweather_bronze"