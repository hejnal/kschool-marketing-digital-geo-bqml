#standardSQL  
-- sample query for one feature
SELECT
  "instrumentalness" AS col_name,  -- Assigns the column name "instrumentalness" to the alias "col_name"
  MIN(instrumentalness) AS min_value,  -- Calculates the minimum value of the "instrumentalness" column and assigns it to "min_value"
  MAX(instrumentalness) AS max_value,  -- Calculates the maximum value of the "instrumentalness" column and assigns it to "max_value"
  AVG(instrumentalness) AS avg_value,  -- Calculates the average value of the "instrumentalness" column and assigns it to "avg_value"
  STDDEV(instrumentalness) AS std_dev,  -- Calculates the standard deviation of the "instrumentalness" column and assigns it to "std_dev"
  VARIANCE(instrumentalness) AS variance,  -- Calculates the variance of the "instrumentalness" column and assigns it to "variance"
  COUNTIF(instrumentalness IS NULL) AS num_nulls  -- Counts the number of null values in the "instrumentalness" column and assigns it to "num_nulls"
FROM
  `<dataset_id>.<table_name>`;  -- Specifies the dataset and table to query from

-- sample query to have one feature histogram
WITH buckets AS (  -- Creates a common table expression (CTE) named "buckets"
  SELECT
    "instrumentalness" AS col_name,  -- Assigns the column name "instrumentalness" to the alias "col_name"
    ML.BUCKETIZE(  -- Applies the BUCKETIZE function to the "instrumentalness" column
      instrumentalness,
      [0.1,  -- Defines the bucket boundaries
      0.2,
      0.3,
      0.4,
      0.5,
      0.6,
      0.7,
      0.8,
      0.9],
      FALSE  -- Specifies that the buckets are not inclusive of the upper bound
    ) AS bucket  -- Assigns the result of BUCKETIZE to the alias "bucket"
  FROM
    `<dataset_id>.<table_name>`  -- Specifies the dataset and table to query from
)
SELECT
  col_name,  -- Selects the "col_name" column
  bucket,  -- Selects the "bucket" column
  COUNT(*) AS num_rows  -- Counts the number of rows in each bucket and assigns it to "num_rows"
FROM
  buckets  -- Queries the "buckets" CTE
GROUP BY
  1,  -- Groups the results by "col_name"
  2;  -- Groups the results by "bucket"

-- create a stored procedure
CREATE
OR REPLACE PROCEDURE < dataset_id >.PrepareFeatureStatistics(  -- Creates or replaces a stored procedure named "PrepareFeatureStatistics"
  IN dataset_id STRING,  -- Defines an input parameter "dataset_id" of type STRING
  IN target_table_name STRING  -- Defines an input parameter "target_table_name" of type STRING
) BEGIN DECLARE feature_columns ARRAY < STRING >;  -- Declares a variable "feature_columns" of type ARRAY<STRING>

DECLARE x INT64 DEFAULT 0;  -- Declares a variable "x" of type INT64 and initializes it to 0

DECLARE target_table_statistics_name STRING;  -- Declares a variable "target_table_statistics_name" of type STRING

DECLARE target_table_histograms_name STRING;  -- Declares a variable "target_table_histograms_name" of type STRING

SET
  target_table_statistics_name = CONCAT(target_table_name, "_statistics");  -- Concatenates the "target_table_name" with "_statistics" and assigns it to "target_table_statistics_name"

SET
  target_table_histograms_name = CONCAT(target_table_name, "_histograms");  -- Concatenates the "target_table_name" with "_histograms" and assigns it to "target_table_histograms_name"

-- create an empty statistics table
EXECUTE IMMEDIATE CONCAT(  -- Executes a dynamic SQL statement
  "CREATE OR REPLACE TABLE `",
  dataset_id,
  ".",
  target_table_statistics_name,
  "`(col_name STRING, min_value FLOAT64, max_value FLOAT64, avg_value FLOAT64, std_dev FLOAT64, variance FLOAT64, apx_quantiles STRING, num_rows INT64, num_nulls INT64, correlation_with_target FLOAT64)"  -- Creates or replaces a table named "target_table_statistics_name" with the specified schema
);

-- create an empty historgrams table
EXECUTE IMMEDIATE CONCAT(  -- Executes a dynamic SQL statement
  "CREATE OR REPLACE TABLE `",
  dataset_id,
  ".",
  target_table_histograms_name,
  "`(col_name STRING, bucket STRING, num_rows INT64)"  -- Creates or replaces a table named "target_table_histograms_name" with the specified schema
);

-- get feature columns
EXECUTE IMMEDIATE CONCAT(  -- Executes a dynamic SQL statement
  "SELECT ARRAY_AGG(column_name) FROM `",
  dataset_id,
  "`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '",
  target_table_name,
  "' AND column_name NOT IN ('id', 'split_col') AND (data_type = 'FLOAT64' OR data_type = 'FLOAT')"  -- Selects all column names from the "target_table_name" that are not "id" or "split_col" and have data type "FLOAT64" or "FLOAT"
) INTO feature_columns;  -- Assigns the result of the query to "feature_columns"

LOOP IF x >= ARRAY_LENGTH(feature_columns) THEN BREAK;  -- Loops through the "feature_columns" array and breaks the loop if "x" is greater than or equal to the length of the array

END IF;

-- insert data to statistics table
EXECUTE IMMEDIATE CONCAT(  -- Executes a dynamic SQL statement
  "INSERT ",
  dataset_id,
  ".",
  target_table_statistics_name,
  "(col_name, min_value, max_value, avg_value, std_dev, variance, apx_quantiles, num_rows, num_nulls, correlation_with_target) SELECT ? AS col_name",  -- Inserts data into the "target_table_statistics_name" table
  ", MIN(",
  feature_columns [
  OFFSET
    (x)],  -- Selects the column name at index "x" from "feature_columns"
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
  ", 5) FROM `",
  dataset_id,
  ".",
  target_table_name,
  "`)) AS q) AS apx_quantiles, COUNT(*) AS num_rows, COUNTIF(",
  feature_columns [
  OFFSET
    (x)],
  " IS NULL) AS num_nulls, CORR(popularity, ",
  feature_columns [
  OFFSET
    (x)],
  ") AS correlation_with_target FROM `",
  dataset_id,
  ".",
  target_table_name,
  "`"  -- Queries the "target_table_name" table to calculate the statistics for the current column
) USING feature_columns [
OFFSET
  (x)];  -- Uses the column name at index "x" from "feature_columns" as a parameter for the query

-- insert data to histograms table
EXECUTE IMMEDIATE CONCAT(  -- Executes a dynamic SQL statement
  "INSERT ",
  dataset_id,
  ".",
  target_table_histograms_name,
  "(col_name, bucket, num_rows) WITH buckets AS (SELECT ? AS col_name, ML.BUCKETIZE(",
  feature_columns [
  OFFSET
    (x)],
  ",[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9], FALSE) AS bucket FROM `",
  dataset_id,
  ".",
  target_table_name,
  "`)",  -- Inserts data into the "target_table_histograms_name" table
  "SELECT col_name, bucket, COUNT(*) AS num_rows FROM buckets GROUP BY 1, 2"  -- Queries the "buckets" CTE to calculate the histogram for the current column
) USING feature_columns [
OFFSET
  (x)];  -- Uses the column name at index "x" from "feature_columns" as a parameter for the query

SET
  x = x + 1;  -- Increments "x" by 1

END LOOP;

END;

-- run the stored procedure
BEGIN DECLARE dataset_id STRING DEFAULT NULL;  -- Declares a variable "dataset_id" of type STRING and initializes it to NULL

DECLARE target_table_name STRING DEFAULT NULL;  -- Declares a variable "target_table_name" of type STRING and initializes it to NULL

SET
  dataset_id = "<dataset_id>";  -- Assigns the value "<dataset_id>" to "dataset_id"

SET
  target_table_name = "<table_name>";  -- Assigns the value "<table_name>" to "target_table_name"

CALL `<dataset_id>.PrepareFeatureStatistics`(dataset_id, target_table_name);  -- Calls the "PrepareFeatureStatistics" stored procedure with the specified parameters

END;