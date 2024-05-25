#standardSQL

-- Basic select: This query selects the unique island names from the penguins dataset.
SELECT
  island
FROM
  `bigquery-public-data.ml_datasets.penguins`;

-- Basic where clause: This query selects all columns for penguins where the sex is 'FEMALE'.
SELECT
  *
FROM
  `bigquery-public-data.ml_datasets.penguins`
WHERE
  sex = 'FEMALE';

-- Basic count: This query counts the total number of female penguins.
SELECT
  COUNT(*) AS num_female_penguins 
FROM
  `bigquery-public-data.ml_datasets.penguins`
WHERE
  sex = 'FEMALE';

-- Basic count per sex: This query counts the number of penguins for each sex, orders them descending.
SELECT
  sex,
  COUNT(*) AS sex_total
FROM
  `bigquery-public-data.ml_datasets.penguins`
GROUP BY
  sex
ORDER BY
  sex_total DESC;  -- Order by the calculated count in descending order

-- IN operator: This query filters to only 'MALE' and 'FEMALE' penguins, then counts per sex.
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
  sex_total DESC;

-- With clause (left join): This query creates a temporary table 'island_details' and then left joins it with the main penguins table to add country information.
WITH island_details AS (
  SELECT
    'Biscoe' AS island,
    'US' AS country
  UNION ALL
  SELECT
    'Torgersen' AS island,
    'Antartic' AS country
)
SELECT
  l.*,   -- Select all columns from the penguins table (l)
  r.country  -- Add the country column from the island_details table (r)
FROM
  `bigquery-public-data.ml_datasets.penguins` l
LEFT JOIN island_details r ON l.island = r.island;  -- Left join to keep all penguins, even if no country match


-- Inner join: This query uses the same island_details table, but performs an inner join to only keep penguins on the specified islands, then counts penguins per island and country.
WITH island_details AS (
  SELECT
    'Biscoe' AS island,
    'US' AS country
  UNION ALL
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
JOIN island_details r ON l.island = r.island  -- Inner join, only keep matching islands
GROUP BY
  1,  
  2
ORDER BY
  num_penguins DESC;

-- Create array: This query simply creates a sample array of tags.
SELECT
  ['black', 'happy', 'white'] AS tags;

-- Query with tags: This creates a temporary table 'tags' with island-tag associations, then joins it to the penguins table.
WITH tags AS (
  SELECT
    'Dream' AS island,
    ['black', 'happy', 'white'] AS tags
  UNION ALL
  SELECT
    'Torgersen' AS island,
    ['gray', 'nostalgic'] AS tags
)
SELECT
  l.*,
  r.tags 
FROM
  `bigquery-public-data.ml_datasets.penguins` l
JOIN tags r ON l.island = r.island
LIMIT 10; 

-- Unnesting tags: This query uses the UNNEST function to flatten the 'tags' array, so each tag is on a separate row. 
WITH tags AS (
  SELECT
    'Dream' AS island,
    ['black', 'happy', 'white'] AS tags
  UNION ALL
  SELECT
    'Torgersen' AS island,
    ['gray', 'nostalgic'] AS tags
)
SELECT
  l.*,
  tag  -- Extract individual tags using UNNEST
FROM
  `bigquery-public-data.ml_datasets.penguins` l
JOIN tags r ON l.island = r.island
JOIN UNNEST(tags) AS tag  -- Flatten the tags array
LIMIT 10;