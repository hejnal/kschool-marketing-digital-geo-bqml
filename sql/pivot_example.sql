#standardSQL
SELECT
  *
FROM
  (
    -- #1 from_item: This subquery extracts the year from the creation_date and splits the tags string into individual tags.
    SELECT
      tag,
      EXTRACT(
        YEAR
        from
          creation_date
      ) AS year
    FROM
      `bigquery-public-data.stackoverflow.posts_questions`,
      UNNEST(SPLIT(tags, '|')) AS tag
    WHERE
      tags IS NOT null
  ) PIVOT (
    -- #2 aggregate: This clause specifies the aggregation function to be used for pivoting.
    COUNT(*) AS n -- #3 pivot_column: This is the name of the column that will hold the aggregated values.
    FOR year in (
      2011,
      2012,
      2013,
      2014,
      2015,
      2016,
      2017,
      2018,
      2019,
      2020
    )
  )