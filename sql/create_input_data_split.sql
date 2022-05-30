#standardSQL
CREATE OR REPLACE TABLE
  <dataset_id>.spotify_dataset_ready_for_split AS (
  SELECT
    *,
    CASE( MOD(ABS(FARM_FINGERPRINT(TO_JSON_STRING(spotify_dataset))), 10))
      WHEN 9 THEN 'test'
      WHEN 8 THEN 'validation'
    ELSE
    'training'
  END
    AS split_col
  FROM
    `<project_id>.<dataset_id>.spotify_dataset` spotify_dataset );