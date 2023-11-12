WITH
  ranked_cities AS (
  SELECT
    city AS city_name,
    geography AS geometry,
    RANK() OVER(PARTITION BY city ORDER BY ST_NUMPOINTS(geography) DESC) AS rank
  FROM
    `<dataset>.<geo_maps_table>`),
  main_cities AS (
  SELECT
    LOWER(city_name) AS city_name,
    geometry
  FROM
    ranked_cities
  WHERE
    rank = 1),
  event_cities AS (
  SELECT
    DISTINCT LOWER(city) AS city
  FROM
    `<dataset>.<indie_labels_ga_data_ready_for_modelling_table>`),
  concert_venues AS (
  SELECT
    LOWER(city_name) AS city,
    geography AS geometry
  FROM
    `<dataset>.<concert_venues_table>` ),
  distance_from_venues AS (
  SELECT
    e.city AS event_city,
    v.city AS venue_city,
    ROUND(ST_DISTANCE(m.geometry, v.geometry) / 1000, 2) AS distance_km
  FROM
    event_cities e
  LEFT JOIN
    main_cities m
  ON
    e.city = LOWER(m.city_name)
  CROSS JOIN
    concert_venues v ),
  distance_statistics AS (
  SELECT
    event_city,
    ROUND(AVG(distance_km), 2) AS avg_distance_km,
    ROUND(MIN(distance_km), 2) AS min_distance_km,
    ROUND(MAX(distance_km), 2) AS max_distance_km,
    COUNT(*) num_connections,
  FROM
    distance_from_venues
  WHERE
    distance_km IS NOT NULL
  GROUP BY
    1)
SELECT
  *
FROM
  distance_statistics
WHERE
  min_distance_km < 1000
ORDER BY
  2 ASC;