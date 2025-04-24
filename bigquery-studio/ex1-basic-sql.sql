-- Table to use: `bigquery-public-data.ml_datasets.penguins`

--Question: How many penguins are there in total in the dataset?

SELECT COUNT(*) FROM `bigquery-public-data.ml_datasets.penguins`;

--Question: What are the different species of penguins in this dataset, and how many of each species are there?

SELECT species, count(*) FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY species;

--Question: Which island has the highest number of penguins?

SELECT island, count(species) as counter
FROM `bigquery-public-data.ml_datasets.penguins`
GROUP BY island
order by counter desc
limit 1


--Question: What is the average flipper length of penguins on 'Biscoe' island compared to the average on 'Dream' island?

select avg(t1.flipper_length_mm) - avg(t2.flipper_length_mm)
from `bigquery-public-data.ml_datasets.penguins` t1,
`bigquery-public-data.ml_datasets.penguins` t2
where t1.island = "Biscoe"
and t2.island = "Dream"

--Question: Can you find the maximum body mass recorded for each species of penguin?

select 

--Question: Identify the top 5 heaviest penguins (by body mass) along with their species, island, and sex.

--Question: Find all unique pairs of islands that share at least one penguin species in common. For each pair, list the islands and one example of a species they share.

--Question: Find the second heaviest penguin on each island.