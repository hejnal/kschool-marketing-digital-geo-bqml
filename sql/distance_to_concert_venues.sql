-- This CTE defines a ranked table of cities based on the number of points in their geometry.
-- It uses the RANK() window function to assign a rank to each city within each name group.
-- The city with the highest number of points in its geometry gets rank 1.
WITH ranked_cities AS (
  SELECT
    NAME AS city_name,
    geometry AS geometry,
    RANK() OVER(
      PARTITION BY NAME -- Partition by city name to rank cities with the same name separately
      ORDER BY
        ST_NUMPOINTS(geometry) DESC -- Order by the number of points in the geometry in descending order
    ) AS rank
  FROM
    `<curated_dataset>.geo_maps`
),

-- This CTE selects the main city for each name, which is the city with the highest number of points in its geometry.
-- It uses the rank from the previous CTE to filter for the city with rank 1.
main_cities AS (
  SELECT
    LOWER(city_name) AS city_name, -- Convert city name to lowercase for case-insensitive comparison
    geometry
  FROM
    ranked_cities
  WHERE
    rank = 1
),

-- This CTE extracts distinct cities from the events table, converting them to lowercase for consistency.
event_cities AS (
  SELECT
    DISTINCT LOWER(city) AS city
  FROM
    `<curated_dataset>.indie_label_events_ready_for_modelling`
),

-- This CTE selects the city and geometry from the concert venues table, converting the city name to lowercase.
concert_venues AS (
  SELECT
    LOWER(city_name) AS city,
    geometry AS geometry
  FROM
    `<curated_dataset>.concert_venues`
),

-- This CTE calculates the distance between each event city and all concert venues.
-- It uses a LEFT JOIN to connect event cities with their corresponding main city from the main_cities CTE.
-- Then, it uses a CROSS JOIN to connect each event city with all concert venues.
-- Finally, it calculates the distance using ST_DISTANCE and converts it to kilometers.
distance_from_venues AS (
  SELECT
    e.city AS event_city,
    v.city AS venue_city,
    ROUND(ST_DISTANCE(m.geometry, v.geometry) / 1000, 2) AS distance_km -- Calculate distance in kilometers and round to 2 decimal places
  FROM
    event_cities e
    LEFT JOIN main_cities m ON e.city = LOWER(m.city_name) -- Join event cities with their corresponding main city
    CROSS JOIN concert_venues v -- Join each event city with all concert venues
),

-- This CTE calculates the average, minimum, maximum distance, and number of connections for each event city.
-- It filters out rows with NULL distance values and groups the results by event city.
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
    distance_km IS NOT NULL -- Filter out rows with NULL distance values
  GROUP BY
    1
)

-- This final SELECT statement retrieves all columns from the distance_statistics CTE.
-- It filters for cities with a minimum distance less than 1000 kilometers and orders the results by average distance in ascending order.
SELECT
  *
FROM
  distance_statistics
WHERE
  min_distance_km < 1000 -- Filter for cities with minimum distance less than 1000 kilometers
ORDER BY
  2 ASC; -- Order by average distance in ascending order