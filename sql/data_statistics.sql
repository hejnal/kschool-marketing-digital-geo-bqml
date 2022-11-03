#standardSQL  
-- sample query for one feature
SELECT
  "instrumentalness" AS col_name,
  MIN(instrumentalness) AS min_value,
  MAX(instrumentalness) AS max_value,
  AVG(instrumentalness) AS avg_value,
  STDDEV(instrumentalness) AS std_dev,
  VARIANCE(instrumentalness) AS variance,
  COUNTIF(instrumentalness IS NULL) AS num_nulls
FROM
  `<project_id>.<dataset_id>.<table_name>`;

-- sample query to have one feature histogram
WITH buckets AS (
  SELECT
    "instrumentalness" AS col_name,
    ML.BUCKETIZE(
      instrumentalness,
      [0.1,
      0.2,
      0.3,
      0.4,
      0.5,
      0.6,
      0.7,
      0.8,
      0.9],
      FALSE
    ) AS bucket
  FROM
    `<project_id>.<dataset_id>.<table_name>`
)
SELECT
  col_name,
  bucket,
  COUNT(*) AS num_rows
FROM
  buckets
GROUP BY
  1,
  2;

-- create a stored procedure
CREATE
OR REPLACE PROCEDURE <dataset_id>.PrepareFeatureStatistics(
  IN dataset_id STRING,
  IN target_table_name STRING
) BEGIN DECLARE feature_columns ARRAY <STRING>;

DECLARE x INT64 DEFAULT 1;

DECLARE target_table_statistics_name STRING;

DECLARE target_table_histograms_name STRING;

SET
  target_table_statistics_name = CONCAT(target_table_name, "_statistics");

SET
  target_table_histograms_name = CONCAT(target_table_name, "_histograms");

-- create an empty statistics table
EXECUTE IMMEDIATE CONCAT(
  "CREATE OR REPLACE TABLE ",
  dataset_id,
  ".",
  target_table_statistics_name,
  "(col_name STRING, min_value FLOAT64, max_value FLOAT64, avg_value FLOAT64, std_dev FLOAT64, variance FLOAT64, apx_quantiles STRING, num_rows INT64, num_nulls INT64, correlation_with_target FLOAT64)"
);

-- create an empty historgrams table
EXECUTE IMMEDIATE CONCAT(
  "CREATE OR REPLACE TABLE ",
  dataset_id,
  ".",
  target_table_histograms_name,
  "(col_name STRING, bucket STRING, num_rows INT64)"
);

-- get feature columns
EXECUTE IMMEDIATE CONCAT(
  "SELECT ARRAY_AGG(column_name) FROM ",
  dataset_id,
  ".INFORMATION_SCHEMA.COLUMNS WHERE table_name = '",
  target_table_name,
  "' AND column_name NOT IN ('id', 'split_col') AND data_type = 'FLOAT64'"
) INTO feature_columns;

LOOP IF x >= ARRAY_LENGTH(feature_columns) THEN BREAK;

END IF;

-- insert data to statistics table
EXECUTE IMMEDIATE CONCAT(
  "INSERT ",
  dataset_id,
  ".",
  target_table_statistics_name,
  "(col_name, min_value, max_value, avg_value, std_dev, variance, apx_quantiles, num_rows, num_nulls, correlation_with_target) SELECT ? AS col_name",
  ", MIN(",
  feature_columns [
  OFFSET
    (x)],
  ") AS min_value, MAX(",
  feature_columns [
  OFFSET
    (x)],
  ") AS max_value, AVG(",
  feature_columns [
  OFFSET
    (x)],
  ") AS avg_value, STDDEV(",
  feature_columns [
  OFFSET
    (x)],
  ") AS std_dev, VARIANCE(",
  feature_columns [
  OFFSET
    (x)],
  ") AS variance, (SELECT STRING_AGG(CAST(ROUND(q, 2) AS STRING), '\t') FROM UNNEST((SELECT APPROX_QUANTILES(",
  feature_columns [
  OFFSET
    (x)],
  ", 5) FROM ",
  dataset_id,
  ".",
  target_table_name,
  ")) AS q) AS apx_quantiles, COUNT(*) AS num_rows, COUNTIF(",
  feature_columns [
  OFFSET
    (x)],
  " IS NULL) AS num_nulls, CORR(popularity, ",
  feature_columns [
  OFFSET
    (x)],
  ") AS correlation_with_target FROM ",
  dataset_id,
  ".",
  target_table_name
) USING feature_columns [
OFFSET
  (x)];

-- insert data to histograms table
EXECUTE IMMEDIATE CONCAT(
  "INSERT ",
  dataset_id,
  ".",
  target_table_histograms_name,
  "(col_name, bucket, num_rows) WITH buckets AS (SELECT ? AS col_name, ML.BUCKETIZE(",
  feature_columns [
  OFFSET
    (x)],
  ",[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9], FALSE) AS bucket FROM ",
  dataset_id,
  ".",
  target_table_name,
  ")",
  "SELECT col_name, bucket, COUNT(*) AS num_rows FROM buckets GROUP BY 1, 2"
) USING feature_columns [
OFFSET
  (x)];

SET
  x = x + 1;

END LOOP;

END;

-- run the stored procedure
DECLARE dataset_id STRING DEFAULT NULL;

DECLARE target_table_name STRING DEFAULT NULL;

SET
  dataset_id = "<dataset_id>";

SET
  target_table_name = "<table_name>";

CALL `<project_id>.<dataset_id>.PrepareFeatureStatistics`(dataset_id, target_table_name);