-- Table to use: `bigquery-public-data.ml_datasets.penguins`

--Question: How many penguins are there in total in the dataset?
SELECT COUNT(*) AS total_penguins
FROM `bigquery-public-data.ml_datasets.penguins`;
--Question: What are the different species of penguins in this dataset, and how many of each species are there?
SELECT species, COUNT(*) AS total_penguins
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY species;
--Question: Which island has the highest number of penguins?
SELECT island, COUNT(*) AS num_penguins
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY island
ORDER BY num_penguins DESC
LIMIT 1;
--Question: What is the average culmen length for each species of penguin?
SELECT AVG(culmen_length_mm)
FROM `bigquery-public-data.ml_datasets.penguins`;
--Question: What is the average flipper length of penguins on 'Biscoe' island compared to the average on 'Dream' island?
SELECT island, AVG(flipper_length_mm)
FROM `bigquery-public-data.ml_datasets.penguins`
WHERE island IN ('Biscoe', 'Dream')
GROUP BY island; -- Se podría poner 1 (el indice de island)
--Question: Can you find the maximum body mass recorded for each species of penguin?
SELECT species, MAX(body_mass_g)
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY species;
--Question: Identify the top 5 heaviest penguins (by body mass) along with their species, island, and sex.
SELECT species, island, sex, MAX (body_mass_g) AS max_body_mass_g
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY 1, 2, 3
ORDER BY MAX (body_mass_g) DESC
LIMIT 5;
--Question: Find all unique pairs of islands that share at least one penguin species in common. For each pair, list the islands and one example of a species they share.
WITH species_map AS (
  SELECT DISTINCT island, species
  FROM `bigquery-public-data.ml_datasets.penguins`
)
SELECT l.island, r.island, l.species
FROM species_map l
JOIN species_map r 
ON l.species = r.species AND l.island < r.island;

--Question: Find the second heaviest penguin on each island.
WITH ranked_penguins AS(
  SELECT island, body_mass_g,
  RANK() OVER(PARTITION BY island ORDER BY body_mass_g DESC) AS rank
  FROM `bigquery-public-data.ml_datasets.penguins`
)
SELECT *
FROM ranked_penguins
WHERE rank = 2;