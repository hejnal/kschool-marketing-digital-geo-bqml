#standardSQL
-- basic select
SELECT
  island
FROM
  `bigquery-public-data.ml_datasets.penguins`;

-- basic where clause
SELECT
  *
FROM
  `bigquery-public-data.ml_datasets.penguins`
WHERE
  sex = 'FEMALE';

-- basic count
SELECT
  COUNT(*)
FROM
  `bigquery-public-data.ml_datasets.penguins`
WHERE
  sex = 'FEMALE';

-- basic count per sex
SELECT
  sex,
  COUNT(*) AS sex_total
FROM
  `bigquery-public-data.ml_datasets.penguins`
GROUP BY
  sex
ORDER BY
  2 DESC;

-- IN operator
SELECT
  sex,
  COUNT(*) AS sex_total
FROM
  `bigquery-public-data.ml_datasets.penguins`
WHERE
  sex IN ('MALE', 'FEMALE')
GROUP BY
  sex
ORDER BY
  2 DESC;

-- with clause (left join)
WITH island_details AS (
  SELECT
    'Biscoe' AS island,
    'US' AS country
  UNION
  ALL
  SELECT
    'Torgersen' AS island,
    'Antartic' AS country
)
SELECT
  l.*,
  r.country
FROM
  `bigquery-public-data.ml_datasets.penguins` l
  LEFT JOIN island_details r ON l.island = r.island;

-- inner join
WITH island_details AS (
  SELECT
    'Biscoe' AS island,
    'US' AS country
  UNION
  ALL
  SELECT
    'Torgersen' AS island,
    'Antartic' AS country
)
SELECT
  l.island,
  r.country,
  COUNT(*) AS num_penguins
FROM
  `bigquery-public-data.ml_datasets.penguins` l
  JOIN island_details r ON l.island = r.island
GROUP BY
  1,
  2
ORDER BY
  3 DESC;

-- create array
SELECT
  ['black', 'happy', 'white'] AS tags;

-- query with tags
WITH tags AS (
  SELECT
    'Dream' AS island,
    ['black',
    'happy',
    'white'] AS tags
  UNION
  ALL
  SELECT
    'Torgersen' AS island,
    ['gray',
    'nostalgic'] AS tags
)
SELECT
  l.*,
  r.tags
FROM
  `bigquery-public-data.ml_datasets.penguins` l
  JOIN tags r ON l.island = r.island
LIMIT
  10;

-- unnesting tags
WITH tags AS (
  SELECT
    'Dream' AS island,
    ['black',
    'happy',
    'white'] AS tags
  UNION
  ALL
  SELECT
    'Torgersen' AS island,
    ['gray',
    'nostalgic'] AS tags
)
SELECT
  l.*,
  tag,
FROM
  `bigquery-public-data.ml_datasets.penguins` l
  JOIN tags r ON l.island = r.island
  JOIN UNNEST(tags) AS tag
LIMIT
  10;