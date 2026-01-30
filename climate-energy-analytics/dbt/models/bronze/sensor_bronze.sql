{{ config(
    schema='bronze',
    materialized='view'
) }}

SELECT
  json_extract_scalar(json_payload, '$.data.meter_id') AS meter_id,
  CAST(json_extract_scalar(json_payload, '$.data.power_kw') AS double) AS power_kw,
  CAST(json_extract_scalar(json_payload, '$.data.voltage') AS int) AS voltage,
  json_extract_scalar(json_payload, '$.data.sensor_id') AS sensor_id,
  CAST(json_extract_scalar(json_payload, '$.data.temperature_c') AS double) AS temp_c,
  CAST(json_extract_scalar(json_payload, '$.data.humidity_pct') AS double) AS humidity_pct,
  json_extract_scalar(json_payload, '$.data.location') AS location,
  CAST(json_extract_scalar(json_payload, '$.data.timestamp') AS bigint) AS epoch_ts,
  json_extract_scalar(json_payload, '$.timestamp') AS ingestion_timestamp
FROM {{ source('bronze', 'sensor_raw') }}
