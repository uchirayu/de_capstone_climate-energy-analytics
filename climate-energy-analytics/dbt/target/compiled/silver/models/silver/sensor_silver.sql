

SELECT
  location,
  meter_id,
  power_kw,
  voltage,
  (power_kw * 1000) / voltage AS current,
  sensor_id,
  temp_c AS temperature,
  humidity_pct AS humidity,
  from_unixtime(epoch_ts) AS dt_timestamp,
  ingestion_timestamp,
  CAST(from_unixtime(epoch_ts) AS date) AS observation_date
FROM "AwsDataCatalog"."bronze"."sensor_bronze"