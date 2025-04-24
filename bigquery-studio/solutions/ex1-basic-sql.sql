--Question: How many penguins are there in total in the dataset?
SELECT COUNT(*) AS total_penguins
FROM bigquery-public-data.ml_datasets.penguins;

--What are the different species of penguins in this dataset, and how many of each species are there?
SELECT species, COUNT(*) AS count_per_species
FROM bigquery-public-data.ml_datasets.penguins
GROUP BY species;

--Question: Which island has the highest number of penguins?
SELECT island, COUNT(*) AS count_per_island
FROM bigquery-public-data.ml_datasets.penguins
GROUP BY island
ORDER BY count_per_island DESC
LIMIT 1; 

--Question: What is the average flipper length of penguins on 'Biscoe' island compared to the average on 'Dream' island?
SELECT island, AVG(flipper_length_mm) AS avg_flipper_length
FROM bigquery-public-data.ml_datasets.penguins
WHERE island IN ('Biscoe', 'Dream')
GROUP BY island;

--Question: Can you find the maximum body mass recorded for each species of penguin?
SELECT species, MAX(body_mass_g) AS max_body_mass
FROM bigquery-public-data.ml_datasets.penguins
GROUP BY species;

--Question: Identify the top 5 heaviest penguins (by body mass) along with their species, island, and sex.
SELECT species, island, sex, body_mass_g
FROM bigquery-public-data.ml_datasets.penguins
ORDER BY body_mass_g DESC
LIMIT 5;

--Question: Find all unique pairs of islands that share at least one penguin species in common. For each pair, list the islands and one example of a species they share.
WITH island_species AS (
    SELECT DISTINCT island, species
    FROM bigquery-public-data.ml_datasets.penguins
)
SELECT i1.island AS island1, i2.island AS island2, ANY_VALUE(i1.species)
FROM island_species i1
INNER JOIN island_species i2 ON i1.species = i2.species AND i1.island < i2.island # This condition prevents duplicate pairs and ensures that each pair of islands is listed only once, with the island names in alphabetical order.
GROUP BY 1, 2
ORDER BY 1, 2;

--Question: Find the second heaviest penguin on each island.
WITH ranked_penguins AS (
  SELECT
    species,
    island,
    body_mass_g,
    RANK() OVER (PARTITION BY island ORDER BY body_mass_g DESC) AS p_rank
FROM
    `bigquery-public-data.ml_datasets.penguins`
)
SELECT * FROM ranked_penguins
WHERE p_rank = 2;