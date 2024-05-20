#standardSQL
CREATE
OR REPLACE TABLE `<dataset_id>.<table_name>_ready_for_ml` AS (
  SELECT
    *,
    CASE
      (
        MOD(
          ABS(
            FARM_FINGERPRINT(TO_JSON_STRING(raw_features))
          ),
          10
        )
      )
      WHEN 9 THEN 'test'
      WHEN 8 THEN 'validation'
      ELSE 'training'
    END AS split_col
  FROM
    `<curated_dataset_id>.<table_name>` raw_features
);