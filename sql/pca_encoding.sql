CREATE
OR REPLACE MODEL <ml_dataset>.spotify_pca3 OPTIONS(model_type = "PCA", num_principal_components = 3) AS
SELECT
    acousticness,
    danceability,
    energy,
    liveness,
    loudness,
    valence
FROM
    `<dataset>.spotify_dataset`;

CREATE
OR REPLACE MODEL <ml_dataset>.spotify_pca2 OPTIONS(model_type = "PCA", num_principal_components = 2) AS
SELECT
    acousticness,
    danceability,
    energy,
    liveness,
    loudness,
    valence
FROM
    `<dataset>.spotify_dataset`;

SELECT
    *
FROM
    ML.PRINCIPAL_COMPONENT_INFO(MODEL `<ml_dataset>.spotify_pca`);

CREATE
OR REPLACE TABLE prediction_outputs.spotify_clusters_pca_2_2010 AS (
    SELECT
        *
    FROM
        ML.PREDICT(
            model <ml_dataset>.spotify_pca2,
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
                    `<dataset>.spotify_dataset`
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
            model <ml_dataset>.spotify_pca3,
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
                    `<dataset>.spotify_dataset`
                WHERE
                    year = 2010
            )
        )
);