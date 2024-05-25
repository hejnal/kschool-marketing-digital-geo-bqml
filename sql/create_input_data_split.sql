#standardSQL
-- Create or replace a table named `<dataset_id>.<table_name>_ready_for_ml`
CREATE OR REPLACE TABLE `<dataset_id>.<table_name>_ready_for_ml` AS (
  -- Select all columns from the original table and add a new column called 'split_col'
  SELECT
    *,
    -- Calculate the split column based on the hash of the raw features
    CASE
      -- Calculate the modulo of the absolute value of the hash of the raw features (converted to JSON string) by 10
      (
        MOD(
          ABS(
            -- Calculate the hash of the raw features using FARM_FINGERPRINT function
            FARM_FINGERPRINT(TO_JSON_STRING(raw_features))
          ),
          10
        )
      )
      -- If the modulo is 9, assign 'test' to the split_col
      WHEN 9 THEN 'test'
      -- If the modulo is 8, assign 'validation' to the split_col
      WHEN 8 THEN 'validation'
      -- Otherwise, assign 'training' to the split_col
      ELSE 'training'
    END AS split_col
  -- Select data from the original table
  FROM
    `<curated_dataset_id>.<table_name>` raw_features
);