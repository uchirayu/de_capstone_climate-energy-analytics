

SELECT
  state,
  sector,
  price,
  date_parse(period, '%Y-%m') AS dt_timestamp,
  ingestion_timestamp,
  CAST(date_parse(period, '%Y-%m') AS date) AS observation_date
FROM "AwsDataCatalog"."bronze"."eia_bronze"