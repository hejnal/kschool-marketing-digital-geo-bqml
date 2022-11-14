CREATE
OR REPLACE MODEL ml_models.spotify_clusters_more_clusters OPTIONS(
    model_type = "kmeans",
    kmeans_init_method = "kmeans++",
    distance_type = "cosine",
    standardize_features = true,
    num_clusters = 100
) AS
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
    `my-gcp-project-id.raw_data.spotify_dataset`;

CREATE
OR REPLACE TABLE prediction_outputs.spotify_clusters_more_clusters_2010 AS (
    SELECT
        *
    FROM
        ML.PREDICT(
            MODEL `my-gcp-project-id.ml_models.spotify_clusters_more_clusters`,
            (
                SELECT
                    *
                FROM
                    `my-gcp-project-id.raw_data.spotify_dataset`
                WHERE
                    year = 2010
            )
        )
);

WITH artist_clusters AS (
    SELECT
        year,
        REGEXP_REPLACE(
            REGEXP_REPLACE(artists, '\\[\'', ''),
            '\'\\]',
            ''
        ) AS artist,
        CENTROID_ID AS cluster_id,
        COUNT(*) AS songs_num
    FROM
        `my-gcp-project-id.prediction_outputs.spotify_clusters_2010`
    GROUP BY
        1,
        2,
        3
),
ranked_clusters AS (
    SELECT
        year,
        artist,
        cluster_id,
        songs_num,
        ROW_NUMBER() OVER(
            PARTITION BY year,
            artist
            ORDER BY
                songs_num DESC
        ) AS cluster_rank
    FROM
        artist_clusters
)
SELECT
    year,
    artist,
    cluster_id,
    songs_num
FROM
    ranked_clusters
WHERE
    cluster_rank = 1;