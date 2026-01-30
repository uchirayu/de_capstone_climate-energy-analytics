{{ config(
    schema='silver',
    materialized='table',
    format='parquet',
    partitioned_by=['observation_date'],
    external_location='s3://climate-energy-raw-data/silver/eia_silver/'
) }}

SELECT
  state,
  sector,
  price,
  date_parse(period, '%Y-%m') AS dt_timestamp,
  ingestion_timestamp,
  CAST(date_parse(period, '%Y-%m') AS date) AS observation_date
FROM {{ ref('eia_bronze') }}
