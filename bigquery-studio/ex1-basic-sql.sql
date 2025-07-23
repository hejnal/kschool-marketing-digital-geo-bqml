-- Table to use: `bigquery-public-data.ml_datasets.penguins`

--Question: How many penguins are there in total in the dataset?

SELECT COUNT(*) 
FROM `bigquery-public-data.ml_datasets.penguins`;

--Question: What are the different species of penguins in this dataset, and how many of each species are there?

SELECT species, COUNT(*) AS total_species
FROM `bigquery-public-data.ml_datasets.penguins` 
GROUP BY species;

--Question: Which island has the highest number of penguins?

SELECT island, COUNT(*) AS pengins_in_islands
FROM `bigquery-public-data.ml_datasets.penguins` 
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

--Question: What is the average flipper length of penguins on 'Biscoe' island compared to the average on 'Dream' island?

SELECT island, ROUND(AVG(flipper_length_mm), 2) AS average_flipper_length
FROM `bigquery-public-data.ml_datasets.penguins`
WHERE island IN ('Biscoe', 'Dream')
GROUP BY 1;

--Question: Can you find the maximum body mass recorded for each species of penguin?

SELECT species, MAX(body_mass_g) AS max_body_mass
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY 1
ORDER BY 2;

--Question: Identify the top 5 heaviest penguins (by body mass) along with their species, island, and sex.
SELECT (species, island, sex, body_mass_g),
FROM `bigquery-public-data.ml_datasets.penguins`
ORDER BY body_mass_g DESC
LIMIT 5;

--Question: Find all unique pairs of islands that share at least one penguin species in common. For each pair, list the islands and one example of a species they share (there are 3 diff islands; Dream, Biscoe and Torgersen).

SELECT island, COUNT(*) AS diff_islands
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY 1;

WITH island_species AS (
  SELECT DISTINCT island, species
  FROM `bigquery-public-data.ml_datasets.penguins`
)
SELECT l.*, r.*
FROM island_species l
JOIN island_species r
ON l.island < r.island AND l.species = r.species;

--Question: Find the second heaviest penguin on each island.

WITH ranked_penguins AS(
  SELECT species, island, body_mass_g,
  RANK() OVER (PARTITION BY island ORDER BY body_mass_g DESC) AS p_rank
  FROM `bigquery-public-data.ml_datasets.penguins`
)
SELECT * FROM ranked_penguins
WHERE p_rank = 2;




