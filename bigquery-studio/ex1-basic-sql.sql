  -- Table to use: `bigquery-public-data.ml_datasets.penguins`
  --Question: How many penguins are there in total in the dataset?
SELECT
  COUNT(*)
FROM
  `bigquery-public-data.ml_datasets.penguins`;
  --Question: What are the different species of penguins in this dataset, and how many of each species are there?
SELECT
  species,
  COUNT(*)
FROM
  `bigquery-public-data.ml_datasets.penguins`
GROUP BY
  species;
  --Question: Which island has the highest number of penguins?
SELECT
  island,
  COUNT(species) AS counter
FROM
  `bigquery-public-data.ml_datasets.penguins`
GROUP BY
  island
ORDER BY
  counter DESC
LIMIT
  1;
  --Question: What is the average flipper length of penguins on 'Biscoe' island compared to the average on 'Dream' island?
SELECT
  AVG(t1.flipper_length_mm) - AVG(t2.flipper_length_mm)
FROM
  `bigquery-public-data.ml_datasets.penguins` t1,
  `bigquery-public-data.ml_datasets.penguins` t2
WHERE
  t1.island = "Biscoe"
  AND t2.island = "Dream";
  --Question: Can you find the maximum body mass recorded for each species of penguin?
SELECT
  species,
  MAX(body_mass_g)
FROM
  bigquery-public-data.ml_datasets.penguins
GROUP BY
  species;
  --Question: Identify the top 5 heaviest penguins (by body mass) along with their species, island, and sex.
SELECT
  species,
  island,
  sex,
  MAX(body_mass_g)
FROM
  bigquery-public-data.ml_datasets.penguins
GROUP BY
  species,
  island,
  sex
ORDER BY
  MAX(body_mass_g) DESC
LIMIT
  5;
  --Question: Find all unique pairs of islands that share at least one penguin species in common. For each pair, list the islands and one example of a species they share.
SELECT
  DISTINCT t1.island i1,
  t2.island i2,
  t1.species
FROM
  bigquery-public-data.ml_datasets.penguins t1,
  bigquery-public-data.ml_datasets.penguins t2
WHERE
  t1.island < t2.island
  AND t1.species = t2.species
  --Question: Find the second heaviest penguin on each island.
WITH
  Peguins AS (
  SELECT
    Island,
    Species,
    body_mass_g,
    ROW_NUMBER() OVER (PARTITION BY Island ORDER BY body_mass_g DESC) AS number
  FROM
    bigquery-public-data.ml_datasets.penguins)
SELECT
  Island,
  Species,
  body_mass_g
FROM
  Peguins
WHERE
  number = 2