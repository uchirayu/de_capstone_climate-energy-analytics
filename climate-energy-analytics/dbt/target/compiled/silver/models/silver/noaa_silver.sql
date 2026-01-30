

SELECT
  station_id,
  datatype as metric,
  value / 10.0 as metric_value,
  CAST(from_iso8601_timestamp(date_str) AS timestamp) AS dt_timestamp,
  ingestion_timestamp,
  CAST(from_iso8601_timestamp(date_str) AS date) AS observation_date
FROM "AwsDataCatalog"."bronze"."noaa_bronze"