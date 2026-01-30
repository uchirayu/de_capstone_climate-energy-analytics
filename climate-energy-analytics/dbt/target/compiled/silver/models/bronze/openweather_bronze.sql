

SELECT
  json_extract_scalar(json_payload, '$.data.city') AS city,
  CAST(json_extract_scalar(json_payload, '$.data.temperature_c') AS double) AS temperature,
    CAST(json_extract_scalar(json_payload, '$.data.feels_like_c') AS double) AS feelslike_temp,
  CAST(json_extract_scalar(json_payload, '$.data.humidity_pct') AS double) AS humidity,
  CAST(json_extract_scalar(json_payload, '$.data.wind_speed_mps') AS double) AS windspeed,
  CAST(json_extract_scalar(json_payload, '$.data.visibility_m') AS double) AS visibility,
  CAST(json_extract_scalar(json_payload, '$.data.weather_main') AS varchar) AS weather_status,
  CAST(json_extract_scalar(json_payload, '$.data.dt') AS bigint) AS epoch_ts,
  json_extract_scalar(json_payload, '$.timestamp') AS ingestion_timestamp
FROM "AwsDataCatalog"."silver"."openweather_raw"