#standardSQL

SELECT * FROM
(
 -- #1 from_item
 SELECT
   tag,
   EXTRACT(YEAR from creation_date) AS year
 FROM
   `bigquery-public-data.stackoverflow.posts_questions`,
   UNNEST(SPLIT(tags, '|')) AS tag
 WHERE tags IS NOT null
)
PIVOT
(
 -- #2 aggregate
 COUNT(*) AS n
 -- #3 pivot_column
 FOR year in (2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020)
)
