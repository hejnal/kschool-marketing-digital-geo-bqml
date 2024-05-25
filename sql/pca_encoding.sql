-- Create a PCA model with 3 principal components named spotify_pca3
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

-- Create a PCA model with 2 principal components named spotify_pca2
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

-- Get information about the principal components of the spotify_pca model
SELECT
    *
FROM
    ML.PRINCIPAL_COMPONENT_INFO(MODEL `<ml_dataset>.spotify_pca`);

-- Create a table named spotify_clusters_pca_2_2010 to store the predictions from the spotify_pca2 model for songs released in 2010
CREATE
OR REPLACE TABLE prediction_outputs.spotify_clusters_pca_2_2010 AS (
    SELECT
        *
    FROM
        -- Use the ML.PREDICT function to get predictions from the spotify_pca2 model
        ML.PREDICT(
            model <ml_dataset>.spotify_pca2,
            -- Select the features used for prediction (acousticness, danceability, etc.) and other relevant information (name, artist, popularity)
            (
                SELECT
                    acousticness,
                    danceability,
                    energy,
                    liveness,
                    loudness,
                    valence,
                    name,
                    -- Clean the artists column by removing square brackets and single quotes
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(artists, '\\[\'', ''),
                        '\'\\]',
                        ''
                    ) AS artist,
                    popularity
                FROM
                    `<dataset>.spotify_dataset`
                -- Filter the data to only include songs released in 2010
                WHERE
                    year = 2010
            )
        )
);

-- Create a table named spotify_clusters_pca_3_2010 to store the predictions from the spotify_pca3 model for songs released in 2010
CREATE
OR REPLACE TABLE prediction_outputs.spotify_clusters_pca_3_2010 AS (
    SELECT
        *
    FROM
        -- Use the ML.PREDICT function to get predictions from the spotify_pca3 model
        ML.PREDICT(
            model <ml_dataset>.spotify_pca3,
            -- Select the features used for prediction (acousticness, danceability, etc.) and other relevant information (name, artist, popularity)
            (
                SELECT
                    acousticness,
                    danceability,
                    energy,
                    liveness,
                    loudness,
                    valence,
                    name,
                    -- Clean the artists column by removing square brackets and single quotes
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(artists, '\\[\'', ''),
                        '\'\\]',
                        ''
                    ) AS artist,
                    popularity
                FROM
                    `<dataset>.spotify_dataset`
                -- Filter the data to only include songs released in 2010
                WHERE
                    year = 2010
            )
        )
);