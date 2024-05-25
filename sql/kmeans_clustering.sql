-- Create or replace a KMeans model named 'spotify_clusters_more_clusters' in the 'ml_models' dataset.
CREATE
OR REPLACE MODEL ml_models.spotify_clusters_more_clusters OPTIONS(
    -- Specify the model type as KMeans.
    model_type = "kmeans",
    -- Use the kmeans++ initialization method for cluster centroids.
    kmeans_init_method = "kmeans++",
    -- Use cosine distance for calculating similarity between data points.
    distance_type = "cosine",
    -- Standardize features before clustering to ensure equal weighting.
    standardize_features = true,
    -- Set the number of clusters to 100.
    num_clusters = 100
) AS
-- Select the relevant features from the 'spotify_dataset' table.
SELECT
    acousticness,
    danceability,
    duration_ms,
    energy,
    explicit,
    liveness,
    loudness,
    mode,
    speechiness,
    valence,
    key
FROM
    `<dataset>.spotify_dataset`;

-- Create or replace a table named 'spotify_clusters_more_clusters_2010' in the 'prediction_outputs' dataset.
-- This table will store the cluster predictions for songs released in 2010.
CREATE
OR REPLACE TABLE prediction_outputs.spotify_clusters_more_clusters_2010 AS (
    -- Select all columns from the prediction results.
    SELECT
        *
    FROM
        -- Use the ML.PREDICT function to apply the 'spotify_clusters_more_clusters' model to the data.
        ML.PREDICT(
            -- Specify the model to use.
            MODEL `<ml_models_dataset>.spotify_clusters_more_clusters`,
            -- Select the data to predict on, filtering for songs released in 2010.
            (
                SELECT
                    *
                FROM
                    `<dataset>.spotify_dataset`
                WHERE
                    year = 2010
            )
        )
);

-- Create a common table expression (CTE) named 'artist_clusters' to group songs by artist and cluster.
WITH artist_clusters AS (
    -- Select the year, artist, cluster ID, and count of songs for each artist-cluster combination.
    SELECT
        year,
        -- Extract the artist name from the 'artists' column, removing unnecessary characters.
        REGEXP_REPLACE(
            REGEXP_REPLACE(artists, '\\[\'', ''),
            '\'\\]',
            ''
        ) AS artist,
        CENTROID_ID AS cluster_id,
        COUNT(*) AS songs_num
    FROM
        -- Use the 'spotify_clusters_2010' table to get the cluster predictions.
        `<prediction_dataset>.spotify_clusters_2010`
    -- Group the results by year, artist, and cluster ID.
    GROUP BY
        1,
        2,
        3
),
-- Create a CTE named 'ranked_clusters' to rank clusters based on the number of songs.
ranked_clusters AS (
    -- Select the year, artist, cluster ID, song count, and rank for each artist-cluster combination.
    SELECT
        year,
        artist,
        cluster_id,
        songs_num,
        -- Use the ROW_NUMBER window function to assign a rank based on the song count.
        ROW_NUMBER() OVER(
            -- Partition the results by year and artist.
            PARTITION BY year,
            artist
            -- Order the results in descending order of song count.
            ORDER BY
                songs_num DESC
        ) AS cluster_rank
    FROM
        -- Use the 'artist_clusters' CTE as the source data.
        artist_clusters
)
-- Select the year, artist, cluster ID, and song count for the top-ranked cluster for each artist.
SELECT
    year,
    artist,
    cluster_id,
    songs_num
FROM
    -- Use the 'ranked_clusters' CTE as the source data.
    ranked_clusters
WHERE
    -- Filter for the top-ranked cluster (cluster_rank = 1).
    cluster_rank = 1;