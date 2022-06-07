CREATE
OR REPLACE MODEL ml_models.spotify_pca3 OPTIONS(model_type = "PCA", num_principal_components = 3) AS
SELECT
    acousticness,
    danceability,
    energy,
    liveness,
    loudness,
    valence
FROM
    `<project_id>.raw_data.spotify_dataset`;

CREATE
OR REPLACE MODEL ml_models.spotify_pca2 OPTIONS(model_type = "PCA", num_principal_components = 2) AS
SELECT
    acousticness,
    danceability,
    energy,
    liveness,
    loudness,
    valence
FROM
    `<project_id>.raw_data.spotify_dataset`;

SELECT
    *
FROM
    ML.PRINCIPAL_COMPONENT_INFO(MODEL `ml_models.spotify_pca`);

CREATE
OR REPLACE TABLE prediction_outputs.spotify_clusters_pca_2_2010 AS (
    SELECT
        *
    FROM
        ML.PREDICT(
            model ml_models.spotify_pca2,
            (
                SELECT
                    acousticness,
                    danceability,
                    energy,
                    liveness,
                    loudness,
                    valence,
                    name,
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(artists, '\\[\'', ''),
                        '\'\\]',
                        ''
                    ) AS artist,
                    popularity
                FROM
                    `<project_id>.raw_data.spotify_dataset`
                WHERE
                    year = 2010
            )
        )
);

CREATE
OR REPLACE TABLE prediction_outputs.spotify_clusters_pca_3_2010 AS (
    SELECT
        *
    FROM
        ML.PREDICT(
            model ml_models.spotify_pca3,
            (
                SELECT
                    acousticness,
                    danceability,
                    energy,
                    liveness,
                    loudness,
                    valence,
                    name,
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(artists, '\\[\'', ''),
                        '\'\\]',
                        ''
                    ) AS artist,
                    popularity
                FROM
                    `<project_id>.raw_data.spotify_dataset`
                WHERE
                    year = 2010
            )
        )
);