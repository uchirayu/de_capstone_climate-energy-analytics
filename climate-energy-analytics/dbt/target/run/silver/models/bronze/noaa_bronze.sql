create or replace view
    "AwsDataCatalog"."bronze"."noaa_bronze"
  as
    

SELECT
  json_extract_scalar(json_payload, '$.data.station_id') AS station_id,
  json_extract_scalar(json_payload, '$.data.datatype') AS datatype,
  CAST(json_extract_scalar(json_payload, '$.data.value') AS double) AS value,
  json_extract_scalar(json_payload, '$.data.date') AS date_str,
  json_extract_scalar(json_payload, '$.timestamp') AS ingestion_timestamp
FROM "AwsDataCatalog"."silver"."noaa_raw"
