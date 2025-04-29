-- Table to use: `bigquery-public-data.ml_datasets.penguins`

--Question: How many penguins are there in total in the dataset?
-- 344
SELECT
COUNT(*) AS NUM_PIN

FROM clean-silo-405314.demo.penguins;

--Question: What are the different species of penguins in this dataset, and how many of each species are there?
SELECT
species,
COUNT(*) AS NUM

FROM clean-silo-405314.demo.penguins

GROUP BY 1;


--Question: Which island has the highest number of penguins?
SELECT
ISLAND, 
COUNT(*) AS NUM
FROM clean-silo-405314.demo.penguins

GROUP BY 1 ORDER BY 2 DESC;

--Question: What is the average flipper length of penguins on 'Biscoe' island compared to the average on 'Dream' island?
SELECT
ISLAND, 
AVG(flipper_length_mm) AS AVG_FLIPPER_LENGTH

FROM clean-silo-405314.demo.penguins
WHERE ISLAND IN ('Biscoe', 'Dream') -- Ojo, distingue mayusculas y minusculas
GROUP BY 1;

--Question: Can you find the maximum body mass recorded for each species of penguin?
SELECT
SPECIES, 
MAX(body_mass_g) AS MAX_BODY_MASS

FROM clean-silo-405314.demo.penguins

GROUP BY 1
;
 
--Question: Identify the top 5 heaviest penguins (by body mass) along with their species, island, and sex.
SELECT
SPECIES, 
ISLAND,
SEX
FROM clean-silo-405314.demo.penguins

ORDER BY BODY_MASS_G DESC
LIMIT 5;

--Question: Find all unique pairs of islands that share at least one penguin species in common. For each pair, list the islands and one example of a species they share.

WITH PENGUINS_DISTINCT AS(
  SELECT ISLAND, SPECIES
  FROM clean-silo-405314.demo.penguins
  GROUP BY 1,2
)

SELECT A.*, B.*
FROM PENGUINS_DISTINCT A
INNER JOIN clean-silo-405314.demo.penguins B
ON A.ISLAND<B.ISLAND -- No queremos las mismas islas
AND A.SPECIES=B.SPECIES
GROUP BY A.ISLAND, B.ISLAND, A.SPECIES;

--Question: Find the second heaviest penguin on each island.
WITH RANKED_DATA AS (SELECT SPECIES, BODY_MASS_G, RANK() OVER(PARTITION BY ISLAND ORDER BY BODY_MASS_G DESC) AS RANK
FROM clean-silo-405314.demo.penguins 
) 
SELECT * 
FROM RANKED_DATA 
WHERE RANK=2;