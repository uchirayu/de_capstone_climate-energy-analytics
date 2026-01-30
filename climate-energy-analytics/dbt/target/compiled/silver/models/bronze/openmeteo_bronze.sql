

SELECT
  json_extract_scalar(json_payload, '$.data.city') AS city,
  CAST(json_extract_scalar(json_payload, '$.data.temperature_c') AS double) AS temperature,
  CAST(json_extract_scalar(json_payload, '$.data.temperature_min_c') AS double) AS temperature_min,
  CAST(json_extract_scalar(json_payload, '$.data.precipitation_mm') AS double) AS precipitation_mm,
  CAST(json_extract_scalar(json_payload, '$.data.windspeed_max_mps') AS double) AS windspeed,
  json_extract_scalar(json_payload, '$.data.dt') AS date_str,
  json_extract_scalar(json_payload, '$.timestamp') AS ingestion_timestamp
FROM "AwsDataCatalog"."silver"."openmeteo_raw"