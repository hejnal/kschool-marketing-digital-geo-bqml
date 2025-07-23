-- Este fichero contiene ejemplos de código SQL para BigQuery ML, cubriendo PCA, K-Means y ARIMA.
-- Incluye comentarios detallados, una sección de ejercicios y las soluciones correspondientes.

-- ====================================================================
-- Sección 1: Análisis de Componentes Principales (PCA)
-- ====================================================================
-- PCA es una técnica de reducción de dimensionalidad. Aquí creamos dos modelos PCA
-- para reducir las características de audio de un dataset de Spotify.
-- Reemplaza <TU_NOMBRE> con` el ID de tu propio dataset en BigQuery.

-- Crea o reemplaza un modelo PCA con 3 componentes principales.
CREATE
OR REPLACE MODEL `<TU_NOMBRE>.pca3_model` OPTIONS(
  model_type = "PCA", -- Especifica que el modelo es de tipo PCA
  num_principal_components = 3 -- Define el número de componentes principales a extraer
) AS
SELECT
    acousticness,
    danceability,
    energy,
    liveness,
    loudness,
    valence
FROM
    `raw_data.spotify_full_dataset`; -- Selecciona las características de audio para entrenar el modelo

-- Crea o reemplaza un modelo PCA con 2 componentes principales.
CREATE
OR REPLACE MODEL `<TU_NOMBRE>.pca2_model` OPTIONS(
  model_type = "PCA", -- Especifica que el modelo es de tipo PCA
  num_principal_components = 2 -- Define el número de componentes principales a extraer
) AS
SELECT
    acousticness,
    danceability,
    energy,
    liveness,
    loudness,
    valence
FROM
    `raw_data.spotify_full_dataset`; -- Selecciona las características de audio para entrenar el modelo

-- Muestra información sobre los componentes principales del modelo PCA de 2 componentes.
-- Esto puede ayudar a entender cuánto explica cada componente de la varianza original.
SELECT
    *
FROM
    ML.PRINCIPAL_COMPONENT_INFO(MODEL `<TU_NOMBRE>.pca2_model`);

-- Crea o reemplaza una tabla para almacenar los resultados de PCA con 2 componentes para el año 2010.
-- Usamos ML.PREDICT para aplicar el modelo PCA a los datos y obtener las puntuaciones de los componentes.
CREATE
OR REPLACE TABLE `<TU_NOMBRE>.spotify_clusters_pca_2_2010` AS (
    SELECT
        * -- Selecciona todas las columnas resultantes de la predicción
    FROM
        ML.PREDICT(
            model `<TU_NOMBRE>.pca2_model`, -- Especifica el modelo PCA a usar
            (
                SELECT
                    acousticness,
                    danceability,
                    energy,
                    liveness,
                    loudness,
                    valence,
                    artist_name AS artist, -- Incluye nombre del artista para análisis posterior
                    popularity -- Incluye popularidad para análisis posterior
                FROM
                    `raw_data.spotify_full_dataset`
                WHERE
                    year = 2010 -- Filtra los datos para el año 2010
            )
        )
);

-- Crea o reemplaza una tabla para almacenar los resultados de PCA con 3 componentes para el año 2010.
-- Similar al anterior, pero usando el modelo PCA de 3 componentes.
CREATE
OR REPLACE TABLE `<TU_NOMBRE>.spotify_clusters_pca_3_2010` AS (
    SELECT
        * -- Selecciona todas las columnas resultantes de la predicción
    FROM
        ML.PREDICT(
            model `<TU_NOMBRE>.pca3_model`, -- Especifica el modelo PCA a usar
            (
                SELECT
                    acousticness,
                    danceability,
                    energy,
                    liveness,
                    loudness,
                    valence,
                    artist_name AS artist, -- Incluye nombre del artista
                    popularity -- Incluye popularidad
                FROM
                    `raw_data.spotify_full_dataset`
                WHERE
                    year = 2010 -- Filtra los datos para el año 2010
            )
        )
);

-- ====================================================================
-- Sección 2: Ejemplo de K-MEANS
-- ====================================================================
-- K-Means es un algoritmo de clustering no supervisado que agrupa datos en K grupos.
-- Aquí creamos un modelo K-Means para agrupar canciones de Spotify basándose en sus características de audio.
-- `Reemplaza <TU_NOMBRE> con` el ID de tu propio dataset.

-- Crea o reemplaza un modelo K-Means con 100 clusters.
-- reemplaza el dataset_id de salida y kmeans_table_id con los tuyos
CREATE
OR REPLACE MODEL `<TU_NOMBRE>.kmeans_table` OPTIONS(
    model_type = "kmeans", -- Especifica que el modelo es de tipo K-Means
    kmeans_init_method = "kmeans++", -- Método de inicialización de centroides
    distance_type = "cosine", -- Tipo de distancia a usar (coseno)
    standardize_features = true, -- Estandariza las características antes del clustering
    num_clusters = 100 -- Define el número de clusters a crear
) AS
SELECT
    acousticness,
    danceability,
    duration_ms,
    energy,
    liveness,
    loudness,
    mode,
    speechiness,
    valence,
    key
FROM
    `raw_data.spotify_full_dataset`; -- Selecciona las características para el clustering

-- crea clusters para el año 2010 aplicando el modelo K-Means a los datos de ese año.
-- ML.PREDICT asigna cada fila a su cluster más cercano.
CREATE
OR REPLACE TABLE `<TU_NOMBRE>.kmeans_clusters_2010` AS (
    SELECT
        * -- Selecciona todas las columnas, incluyendo la asignación de cluster (CENTROID_ID)
    FROM
        ML.PREDICT(
            MODEL `<TU_NOMBRE>.kmeans_table`, -- Especifica el modelo K-Means a usar
            (
                SELECT
                    * -- Selecciona todas las columnas de los datos de entrada
                FROM
                    `raw_data.spotify_full_dataset`
                WHERE
                    year = 2010 -- Filtra los datos para el año 2010
            )
        )
);

-- muestra el artista más popular (con más canciones) para un clúster dado en el año 2010.
-- Usa CTEs (Common Table Expressions) para organizar el cálculo.
WITH artist_clusters AS (
    -- Agrupa las canciones por año, artista y ID de clúster para contar cuántas canciones tiene cada artista en cada clúster.
    SELECT
        year,
        artist_name AS artist,
        CENTROID_ID AS cluster_id,
        COUNT(*) AS songs_num
    FROM
        `<TU_NOMBRE>.kmeans_clusters_2010` -- Usa la tabla con los resultados del clustering
    GROUP BY
        1, 2, 3
),
ranked_clusters AS (
    -- Clasifica los clústeres para cada artista basándose en el número de canciones.
    SELECT
        year,
        artist,
        cluster_id,
        songs_num,
        ROW_NUMBER() OVER(
            PARTITION BY year, artist -- Particiona por año y artista
            ORDER BY
                songs_num DESC -- Ordena por número de canciones descendente
        ) AS cluster_rank -- Asigna un rango dentro de cada partición
    FROM
        artist_clusters
)
-- Selecciona solo el clúster con el rango 1 para cada artista (el que tiene más canciones).
SELECT
    year,
    artist,
    cluster_id,
    songs_num
FROM
    ranked_clusters
WHERE
    cluster_rank = 1;

-- ====================================================================
-- Sección 3: Ejemplo de ARIMA
-- ====================================================================
-- ARIMA es un modelo de series temporales utilizado para pronosticar valores futuros basándose en datos históricos.
-- Aquí creamos un modelo ARIMA para predecir visitas a una página web.
-- `Reemplaza <TU_NOMBRE> con` el ID de tu propio dataset.

-- ver cómo se ven los datos de visitas agregados por día.
SELECT
  parsed_date,
  SUM(total_visits) AS total_visits
FROM
  raw_data.indie_label_visits_per_day -- Datos de visitas por día
GROUP BY
  1; -- Agrupa por fecha

-- modelo con decompose_time_series
-- Crea o reemplaza un modelo ARIMA+ para predecir visitas (primero crea una table - indie_label_visits_per_day, con fuentes de GA data)
CREATE OR REPLACE MODEL
  `<TU_NOMBRE>.web_visits_arima_model` OPTIONS (
    model_type = "ARIMA_PLUS", -- Especifica el tipo de modelo como ARIMA+
    time_series_timestamp_col = "parsed_date", -- Columna con la marca de tiempo
    time_series_data_col = "total_visits", -- Columna con los datos de la serie temporal
    auto_arima = TRUE, -- Permite a BigQuery ML seleccionar automáticamente los mejores parámetros ARIMA
    data_frequency = "AUTO_FREQUENCY", -- Detecta automáticamente la frecuencia de los datos (diaria, etc.)
    decompose_time_series = TRUE, -- Descompone la serie temporal en sus componentes (tendencia, estacionalidad, etc.)
    holiday_region='US' -- Considera festivos de EE. UU. al modelar la estacionalidad
    ) AS (
  SELECT
    parsed_date,
    SUM(total_visits) AS total_visits
  FROM
    raw_data.indie_label_visits_per_day
  GROUP BY
    1 -- Agrupa por fecha para obtener el total diario
    );

-- evaluar el modelo ARIMA entrenado.
-- ML.ARIMA_EVALUATE proporciona métricas de evaluación del modelo.
SELECT
  *
FROM
  ML.ARIMA_EVALUATE(MODEL `<TU_NOMBRE>.web_visits_arima_model`);

-- realizar una previsión (forecast) de visitas para los próximos 30 días.
-- ML.FORECAST utiliza el modelo ARIMA para predecir valores futuros.
SELECT
  *
FROM
  ML.FORECAST(MODEL `<TU_NOMBRE>.web_visits_arima_model`,
    STRUCT(30 AS horizon, -- Número de pasos de tiempo a predecir (30 días)
      0.8 AS confidence_level)); -- Nivel de confianza para los intervalos de predicción

-- explicar la previsión, mostrando los componentes de la serie temporal para los valores previstos.
-- ML.EXPLAIN_FORECAST proporciona detalles sobre cómo se llegó a la previsión.
SELECT
  *
FROM
  ML.EXPLAIN_FORECAST(MODEL `<TU_NOMBRE>.web_visits_arima_model`,
    STRUCT(30 AS horizon, -- Número de pasos de tiempo de la previsión a explicar
      0.8 AS confidence_level)); -- Nivel de confianza

-- concatenar la serie temporal histórica con la predicción para visualización o análisis combinado.
WITH
  historic_timeseries AS (
  -- Selecciona los datos históricos de visitas.
  SELECT
    parsed_date AS history_timestmap,
    SUM(total_visits) AS history_value
  FROM
    raw_data.indie_label_visits_per_day
  GROUP BY
    1)
-- Une los datos históricos con los datos de previsión usando UNION ALL.
SELECT
  history_timestmap AS timestamp, -- Renombra la columna de fecha histórica a 'timestamp'
  history_value, -- Incluye el valor histórico
  NULL AS forecast_value, -- Establece el valor de previsión como NULL para filas históricas
  NULL AS prediction_interval_lower_bound, -- Intervalo inferior como NULL
  NULL AS prediction_interval_upper_bound -- Intervalo superior como NULL
FROM
  historic_timeseries
UNION ALL -- Combina los resultados de las dos consultas
SELECT
  forecast_timestamp AS timestamp, -- Renombra la columna de fecha de previsión a 'timestamp'
  NULL AS history_value, -- Establece el valor histórico como NULL para filas de previsión
  forecast_value, -- Incluye el valor de previsión
  prediction_interval_lower_bound, -- Incluye el límite inferior del intervalo de predicción
  prediction_interval_upper_bound -- Incluye el límite superior del intervalo de predicción
FROM
  ML.FORECAST(MODEL `<TU_NOMBRE>.web_visits_arima_model`,
    STRUCT(30 AS horizon, -- Previsión para 30 días
      0.8 AS confidence_level)); -- Nivel de confianza

-- ====================================================================
-- Ejercicios
-- ====================================================================
-- Aquí tienes algunos ejercicios basados en el código SQL anterior para practicar tus conocimientos de BigQuery ML y SQL.
-- Recuerda `reemplazar <TU_NOMBRE> con` el ID de tu propio dataset en BigQuery antes de ejecutar cualquier consulta.

-- Ejercicio 1: Consultar los Resultados de PCA (2 Componentes)
-- La tabla `<TU_NOMBRE>.spotify_clusters_pca_2_2010` contiene las puntuaciones de los dos componentes principales
-- para las canciones de Spotify del año 2010, junto con el artista y la popularidad.
-- Escribe una consulta SQL para encontrar las 10 canciones más populares (según la columna `popularity`)
-- en la tabla `<TU_NOMBRE>.spotify_clusters_pca_2_2010`. Muestra el artista, la popularidad y las puntuaciones
-- de los dos componentes principales (`principal_component_1` y `principal_component_2`).

-- Ejercicio 2: Analizar Clusters K-Means (Año Diferente)
-- El código crea una tabla de clusters K-Means para el año 2010 `(<TU_NOMBRE>.kmeans_clusters_2010`).
-- Modifica la consulta que muestra el artista más popular por cluster para que funcione con datos de
-- un año diferente, por ejemplo, el año `2015`. Para ello, asumirás que existe una tabla
-- `<TU_NOMBRE>.kmeans_clusters_2015` con la misma estructura que `<TU_NOMBRE>.kmeans_clusters_2010`.
-- Adapta la CTE `artist_clusters` y las subsiguientes partes de la consulta para usar esta nueva tabla.

-- Ejercicio 3: Consultar la Previsión ARIMA
-- La última parte del código ARIMA concatena los datos históricos con la previsión en una sola salida.
-- Escribe una consulta SQL para mostrar solo los resultados de la previsión (las filas donde `history_value` es NULL)
-- de la salida de la última consulta del apartado ARIMA. Muestra el `timestamp`, el `forecast_value`,
-- `prediction_interval_lower_bound` y `prediction_interval_upper_bound`.

-- ====================================================================
-- Soluciones a los Ejercicios
-- ====================================================================
-- Aquí están las soluciones completas para los ejercicios propuestos.
-- Recuerda `reemplazar <TU_NOMBRE> con` el ID de tu propio dataset.

-- --------------------------------------------------------------------
-- Solución Ejercicio 1: Consultar los Resultados de PCA (2 Componentes)
-- --------------------------------------------------------------------
/*
SELECT
    artist, -- Nombre del artista
    popularity, -- Nivel de popularidad de la canción
    principal_component_1, -- Puntuación del primer componente principal
    principal_component_2 -- Puntuación del segundo componente principal
FROM
    `<TU_NOMBRE>.spotify_clusters_pca_2_2010` -- Tabla con los resultados de PCA
ORDER BY
    popularity DESC -- Ordena por popularidad de forma descendente
LIMIT 10; -- Limita los resultados a las 10 canciones más populares
*/

-- --------------------------------------------------------------------
-- Solución Ejercicio 2: Analizar Clusters K-Means (Año Diferente - 2015)
-- --------------------------------------------------------------------
-- Asumimos que ya existe la tabla `<TU_NOMBRE>.kmeans_clusters_2015`
/*
WITH artist_clusters AS (
    -- Agrupa las canciones por año, artista y ID de clúster para el año 2015
    SELECT
        year,
        artist_name AS artist,
        CENTROID_ID AS cluster_id,
        COUNT(*) AS songs_num
    FROM
        `<TU_NOMBRE>.kmeans_clusters_2015` -- Usa la tabla para el año 2015
    GROUP BY
        1, 2, 3
),
ranked_clusters AS (
    -- Clasifica los clústeres para cada artista basándose en el número de canciones (para 2015)
    SELECT
        year,
        artist,
        cluster_id,
        songs_num,
        ROW_NUMBER() OVER(
            PARTITION BY year, artist
            ORDER BY
                songs_num DESC
        ) AS cluster_rank
    FROM
        artist_clusters
)
-- Selecciona el clúster más popular (con más canciones) para cada artista en 2015.
SELECT
    year,
    artist,
    cluster_id,
    songs_num
FROM
    ranked_clusters
WHERE
    cluster_rank = 1;
*/

-- --------------------------------------------------------------------
-- Solución Ejercicio 3: Consultar la Previsión ARIMA
-- --------------------------------------------------------------------
/*
-- Repetimos la consulta que concatena histórico y previsión, pero esta vez
-- la usamos como subconsulta o CTE para filtrar solo las filas de previsión.
SELECT
    timestamp, -- Marca de tiempo de la previsión
    forecast_value, -- Valor previsto
    prediction_interval_lower_bound, -- Límite inferior del intervalo de predicción
    prediction_interval_upper_bound
FROM (
    -- La consulta original que concatena histórico y previsión
    WITH
      historic_timeseries AS (
      SELECT
        parsed_date AS history_timestmap,
        SUM(total_visits) AS history_value
      FROM
        raw_data.indie_label_visits_per_day
      GROUP BY
        1)
    SELECT
      history_timestmap AS timestamp,
      history_value,
      NULL AS forecast_value,
      NULL AS prediction_interval_lower_bound,
      NULL AS prediction_interval_upper_bound
    FROM
      historic_timeseries
    UNION ALL
    SELECT
      forecast_timestamp AS timestamp,
      NULL AS history_value,
      forecast_value,
      prediction_interval_lower_bound,
      prediction_interval_upper_bound
    FROM
      ML.FORECAST(MODEL `<TU_NOMBRE>.web_visits_arima_model`,
        STRUCT(30 AS horizon,
          0.8 AS confidence_level))
)
WHERE
    history_value IS NULL; -- Filtra las filas donde el valor histórico es NULL (es decir, las filas de previsión)
*/