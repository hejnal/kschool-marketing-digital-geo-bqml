-- Table to use: `bigquery-public-data.ml_datasets.penguins`

--Question: How many penguins are there in total in the dataset?

SELECT COUNT(*) AS total_penguins
FROM `bigquery-public-data`.`ml_datasets`.`penguins`;

--Question: What are the different species of penguins in this dataset, and how many of each species are there?

SELECT 
    species, 
    COUNT(*) AS total_penguins
FROM `bigquery-public-data.ml_datasets.penguins`
WHERE species IS NOT NULL
GROUP BY species
ORDER BY total_penguins DESC;

  
--Question: Which island has the highest number of penguins?

SELECT
    island,
    COUNT(*) AS top_number_of_pinguins
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY island
ORDER BY top_number_of_pinguins DESC
LIMIT 1;

--Question: What is the average flipper length of penguins on 'Biscoe' island compared to the average on 'Dream' island?

SELECT island, AVG(flipper_length_mm) AS flipper_length
FROM `bigquery-public-data.ml_datasets.penguins`
WHERE island IN ('Biscoe', 'Dream')
GROUP BY island
ORDER BY flipper_length DESC;

--Question: Can you find the maximum body mass recorded for each species of penguin?

SELECT species, MAX(body_mass_g) AS max_body_mass
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY species
ORDER BY max_body_mass DESC;

--Question: Identify the top 5 heaviest penguins (by body mass) along with their species, island, and sex.

SELECT
    body_mass_g,
    species,
    island,
    sex
FROM `bigquery-public-data.ml_datasets.penguins`
WHERE body_mass_g IS NOT NULL
ORDER BY body_mass_g DESC
LIMIT 5;

--Question: Find all unique pairs of islands that share at least one penguin species in common. For each pair, list the islands and one example of a species they share.

SELECT t1.island AS island1, t2.island AS island2, t1.species AS shared_species
FROM
  (
    SELECT DISTINCT island, species
    FROM `bigquery-public-data`.`ml_datasets`.`penguins`
    WHERE island IS NOT NULL AND species IS NOT NULL
  ) AS t1
JOIN
  (
    SELECT DISTINCT island, species
    FROM `bigquery-public-data`.`ml_datasets`.`penguins`
    WHERE island IS NOT NULL AND species IS NOT NULL
  ) AS t2
  ON t1.species = t2.species
WHERE t1.island < t2.island
ORDER BY island1, island2, shared_species;

--Question: Find the second heaviest penguin on each island.

SELECT
    island,
    body_mass_g,
    species,
    sex
FROM (
    SELECT
        island,
        body_mass_g,
        species,
        sex,
        ROW_NUMBER() OVER (PARTITION BY island ORDER BY body_mass_g DESC) AS rank
    FROM `bigquery-public-data.ml_datasets.penguins`
    WHERE body_mass_g IS NOT NULL
)
WHERE rank = 2
ORDER BY island;

