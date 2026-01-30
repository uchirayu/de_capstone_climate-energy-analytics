create or replace view
    "AwsDataCatalog"."bronze"."eia_bronze"
  as
    

SELECT
  json_extract_scalar(json_payload, '$.data.state') AS state,
  json_extract_scalar(json_payload, '$.data.sector') AS sector,
  CAST(json_extract_scalar(json_payload, '$.data.price') AS double) AS price,
  json_extract_scalar(json_payload, '$.data.period') AS period,
  json_extract_scalar(json_payload, '$.timestamp') AS ingestion_timestamp
FROM "AwsDataCatalog"."silver"."eia_raw"
