


SELECT
  location,
  temperature_c,
  humidity,
  windspeed_mps,
  'openweather' as source,
  observation_date,
  ingestion_date
FROM "AwsDataCatalog"."silver"."openweather_silver"

UNION ALL

SELECT
  location,
  temperature_c,
  humidity,
  windspeed_mps,
  'openmeteo' as source,
  observation_date,
  ingestion_date
FROM "AwsDataCatalog"."silver"."openmeteo_silver"