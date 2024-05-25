-- This CTE (Common Table Expression) is named "ranked_cities" and will be used to rank cities based on the number of points in their geometry.
WITH ranked_cities AS (
    -- Select the city name, geometry, and calculate the rank for each city.
    SELECT
        -- Alias the "NAME" column as "city_name" for clarity.
        NAME AS city_name,
        geometry,
        -- Use the RANK window function to assign a rank to each city based on the number of points in its geometry.
        -- Partition by "NAME" to rank cities individually.
        -- Order by "ST_NUMPOINTS(geometry)" in descending order to rank cities with more points higher.
        RANK() OVER(
            PARTITION BY NAME
            ORDER BY
                ST_NUMPOINTS(geometry) DESC
        ) AS rank
    -- Select data from the "geo_maps" table in the specified dataset.
    FROM
        `<dataset>.geo_maps`
    -- Filter the data to include only the specified cities (case-insensitive).
    WHERE
        LOWER(NAME) IN (
            ('madrid'),
            'valencia',
            'barcelona',
            'zaragoza'
        )
)
-- Select the city name and geometry from the "ranked_cities" CTE.
SELECT
    city_name,
    geometry
-- Filter the results to include only the cities with the highest rank (rank = 1).
FROM
    ranked_cities
WHERE
    rank = 1;