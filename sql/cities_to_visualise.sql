WITH ranked_cities AS (
    SELECT
        NAME AS city_name,
        geometry,
        RANK() OVER(
            PARTITION BY NAME
            ORDER BY
                ST_NUMPOINTS(geometry) DESC
        ) AS rank
    FROM
        `my-gcp-project-id.city_maps.geo_maps`
    WHERE
        LOWER(NAME) IN (
            ('madrid'),
            'valencia',
            'barcelona',
            'zaragoza'
        )
)
SELECT
    city_name,
    geometry
FROM
    ranked_cities
WHERE
    rank = 1;