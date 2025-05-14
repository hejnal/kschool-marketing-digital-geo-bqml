-- [[ Instrucciones Generales ]]
-- 1. Reemplaza `<TU_NOMBRE>` a lo largo de este script con el nombre que elijas para tu dataset (Puedes usar cmd+D).

-- Para comparar modelos: lo mejor es coger los mismos datos (de evaluacion) y ver 


-- #####################################################################################
-- ## Sección 0: Preparación del Entorno                                              ##
-- #####################################################################################
-- (Como en la respuesta anterior - Crear `tu_dataset`)
CREATE SCHEMA IF NOT EXISTS `<TU_NOMBRE>` -- TODO: Reemplaza <TU_NOMBRE>
OPTIONS(location = 'US');

-- #####################################################################################
-- ## Sección 1: Modelo Baseline - Regresión Logística para predecir `will_buy_later` ##
-- ## (Usando la tabla original `ga_propensidad_compra_ready_for_ml`)                 ##
-- #####################################################################################
-- (Como en la respuesta anterior - Modelo, Evaluación, Predicción para `will_buy_later` con Logistic Regression)
-- Consulta https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-transform
/*
CREATE OR REPLACE MODEL `<TU_NOMBRE>.ga_will_buy_later_logistic_transform` -- TODO: Reemplaza tu_dataset
TRANSFORM(...) OPTIONS(...) AS SELECT ... FROM `<TU_NOMBRE>.ga_propensidad_compra_ready_for_ml` WHERE split_col = 'training';

*/

-- Lo que hago es traerme el modelo que habia hecho en el ejercicio 3 e ir viendo
-- como podria mejorarlo 

-- ML.QUANTILE_BUCKETIZE function, which lets you break a continuous numerical feature into buckets based on quantiles.

CREATE OR REPLACE MODEL `DEMO_CLAUDIA.ga_propensidad_compra_baseline_model_transform`
  TRANSFORM(
   bounces,
   -- time_on_site, -- numerico: vemos su distribucion
   ML.QUANTILE_BUCKETIZE(time_on_site, 10) OVER() AS time_on_site_bucketize,
   -- page_views, -- tambien es numerica, tiene sentifdo hacer bucketize que nos crea particiones (num. que indicamos)
   ML.QUANTILE_BUCKETIZE(page_views, 10) OVER() AS page_views_bucketize,
   -- Para las variables categoricas podemos usar:
   -- Selecciono var. que tenga sentido que creen una unica
   ML.FEATURE_CROSS(STRUCT(source,medium, channel_grouping)) AS origin,
   -- source,
   -- medium,
   -- channel_grouping,
   ML.FEATURE_CROSS(STRUCT(CAST(add_to_cart AS STRING) AS add_to_cart,CAST(product_detail_view AS STRING) AS product_detail_view)) AS behaviour,
   -- add_to_cart,
   -- product_detail_view,
   will_buy_later,
   is_mobile,
   ML.FEATURE_CROSS(STRUCT(city, country)) AS geo
  )
  OPTIONS(
  -- Especifica el tipo de modelo a crear como Regresión Logística.
  model_type = "LOGISTIC_REG",
  -- Divide automáticamente los datos en conjuntos de entrenamiento y evaluación.
  data_split_method = "AUTO_SPLIT",
  -- Define la columna "will_buy_later" como la variable objetivo para la predicción.
  input_label_cols = ["will_buy_later"],
  -- Habilita la explicabilidad global para el modelo, proporcionando información sobre la importancia de las características.
  enable_global_explain = TRUE
) AS
-- Selecciona las características y la variable objetivo del dataset preparado.
SELECT
  bounces,
  time_on_site,
  page_views,
  source,
  medium,
  channel_grouping,
  is_mobile,
  add_to_cart,
  product_detail_view,
  will_buy_later,
  city, -- añadido ahora
  country -- añadido ahora
-- Utiliza el dataset preparado para el entrenamiento, filtrando por filas con split_col = 'training'.
FROM
  `DEMO_CLAUDIA.ga_propensidad_compra_ready_for_ml`
WHERE
  split_col = 'training';

-- Evalua el modelo: Compararemos este modelo y el anterior (ex3)

-- Esta sección evalúa el rendimiento del modelo entrenado utilizando los datos de validación.
SELECT
  *, 'model1' as modelo_evaluado
FROM
  -- Utiliza la función ML.EVALUATE para evaluar el modelo.
  ML.EVALUATE(MODEL `DEMO_CLAUDIA.ga_propensidad_compra_baseline_model`,
    -- Selecciona los datos de validación del dataset preparado.
    (
    SELECT
      bounces,
      time_on_site,
      page_views,
      source,
      medium,
      channel_grouping,
      is_mobile,
      add_to_cart,
      product_detail_view,
      will_buy_later
    FROM
     `DEMO_CLAUDIA.ga_propensidad_compra_ready_for_ml`
    WHERE
      split_col = 'validation'),
    -- Establece el umbral para la clasificación en 0.5.
    STRUCT(0.2 AS threshold)) -- Aqui le ponemos 0.2 en vez de 0.5

  -- Añadimos asi de facil el nuevo modelo  
  UNION ALL
  
  SELECT
  *, 'model2' as modelo_evaluado
  FROM
  -- Utiliza la función ML.EVALUATE para evaluar el modelo.
  ML.EVALUATE(MODEL `DEMO_CLAUDIA.ga_propensidad_compra_baseline_model_transform`,
    -- Selecciona los datos de validación del dataset preparado.
    (
    SELECT
      bounces,
      time_on_site,
      page_views,
      source,
      medium,
      channel_grouping,
      is_mobile,
      add_to_cart,
      product_detail_view,
      will_buy_later,
      city,
      country
    FROM
     `DEMO_CLAUDIA.ga_propensidad_compra_ready_for_ml`
    WHERE
      split_col = 'validation'),
    -- Establece el umbral para la clasificación en 0.5.
    STRUCT(0.2 AS threshold)) -- Aqui le ponemos 0.2 en vez de 0.5
  ;

-- Aqui se ve accuracy altisima -> puede tener que ver con lo desbalanceado del 
-- dataset que por otra parte es natural -> siempre vamos a tener menos compras que el resto
-- de acciones
  
-- Intenta hacer predicciones


-- Solo visualiza las features
SELECT * FROM ML.TRANSFORM(MODEL `tu_dataset.ga_will_buy_later_logistic_transform`, (SELECT * FROM `<TU_NOMBRE>.ga_propensidad_compra_ready_for_ml` WHERE split_col = 'test' LIMIT 10));


-- #####################################################################################
-- ## Sección 2: Experimentar con un Tipo de Modelo Diferente - XGBoost             ##
-- ## (Usando la tabla original `ga_propensidad_compra_ready_for_ml` para `will_buy_later`) ##
-- #####################################################################################
-- (Como en la respuesta anterior - Modelo XGBoost para `will_buy_later`)
/*
CREATE OR REPLACE MODEL `<TU_NOMBRE>.ga_will_buy_later_xgboost_transform` -- TODO: Reemplaza tu_dataset
TRANSFORM(...) OPTIONS(model_type = "BOOSTED_TREE_CLASSIFIER", ...) AS SELECT ... FROM `<TU_NOMBRE>.ga_propensidad_compra_ready_for_ml` WHERE split_col = 'training';
*/

-- #####################################################################################
-- ## Sección 2.5: Preparación de Datos Específicos para Predecir 'Añadir al Carrito' ##
-- #####################################################################################

-- Paso 2.5.1: Crear una tabla optimizada para el análisis de "añadir al carrito".
-- !! ASEGÚRATE DE HABER EJECUTADO ESTA CONSULTA ANTES DE PASAR A LA SECCIÓN 3 !!
-- Esta es la consulta que proporcionaste.
-- Reemplaza `tu_dataset` con tus valores.
CREATE OR REPLACE TABLE `<TU_NOMBRE>.propensidad_anadir_al_carrito` AS ( -- TODO: Reemplaza tu_dataset
  SELECT
    *
  FROM
    (
      SELECT
        PARSE_TIMESTAMP("%Y%m%d", date) AS parsed_date,
        fullVisitorId AS full_visitor_id,
        IFNULL(totals.bounces, 0) AS bounces,
        IFNULL(totals.timeOnSite, 0) AS time_on_site,
        totals.pageviews AS page_views,
        trafficSource.source,
        trafficSource.medium,
        channelGrouping AS channel_grouping,
        device.isMobile AS is_mobile,
        IF (
          (
            SELECT SUM(IF (eCommerceAction.action_type = '3', 1, 0))
            FROM UNNEST(hits)
          ) >= 1, 1, 0
        ) AS add_to_cart, -- Etiqueta objetivo para la Sección 3
        IF (
          (
            SELECT SUM(IF (eCommerceAction.action_type = '2', 1, 0))
            FROM UNNEST(hits)
          ) >= 1, 1, 0
        ) AS product_detail_view,
        IFNULL(geoNetwork.city, "") AS city,
        IFNULL(geoNetwork.country, "") AS country
      FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*` s
      WHERE
        totals.newVisits = 1
        AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801' -- Rango de datos original de tu consulta
    ) main_session
    JOIN (
      SELECT
        fullvisitorid AS full_visitor_id_join,
        IF (
          COUNTIF(new_visit IS NOT NULL AND added_to_cart_future >= 1) > 0, 1, 0
        ) AS will_add_to_cart_later
      FROM
        (
          SELECT
            fullvisitorid,
            totals.newVisits AS new_visit,
            IF(
              (
                SELECT SUM(IF (eCommerceAction.action_type = '3', 1, 0))
                FROM UNNEST(hits)
              ) >= 1, 1, 0
            ) AS added_to_cart_future
          FROM
            `bigquery-public-data.google_analytics_sample.ga_sessions_*`
          WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
        )
      GROUP BY
        fullvisitorid
    ) future_behaviour ON main_session.full_visitor_id = future_behaviour.full_visitor_id_join
);

-- Nota importante sobre la nueva tabla `propensidad_anadir_al_carrito`:
-- 1. Esta tabla NO contiene la columna `split_col`.
-- 2. Para los modelos entrenados con esta tabla, deberás usar `data_split_method = "AUTO_SPLIT"`
--    en las `OPTIONS` del modelo y NO incluir `WHERE split_col = ...` en la subconsulta `AS SELECT ...`.
--    BigQuery ML se encargará de dividir los datos automáticamente.

-- Paso 2.5.2: Verificar la tabla creada (opcional)
-- SELECT * FROM `<TU_NOMBRE>.propensidad_anadir_al_carrito` LIMIT 10; -- TODO: Reemplaza tu_dataset

-- #####################################################################################
-- ## Sección 3: Cambiando el Objetivo - Predecir `add_to_cart` (Sesión Actual)     ##
-- ## (Usando la nueva tabla `propensidad_anadir_al_carrito`)                       ##
-- #####################################################################################

-- Paso 3.1: Crear un nuevo modelo para predecir `add_to_cart` en la sesión actual.
-- Este modelo utilizará la tabla `propensidad_anadir_al_carrito` que preparaste.
-- Elegiremos BOOSTED_TREE_CLASSIFIER como ejemplo.
CREATE OR REPLACE MODEL `<TU_NOMBRE>.ga_add_to_cart_current_session_model` -- TODO: Reemplaza tu_proyecto y tu_dataset
TRANSFORM(
  -- Características numéricas directas o que serán bucketizadas
  bounces,
  is_mobile, -- Ya es 0/1 implícitamente
  ML.QUANTILE_BUCKETIZE(time_on_site, 5) OVER () AS time_on_site_bucketed,
  ML.QUANTILE_BUCKETIZE(page_views, 5) OVER () AS page_views_bucketed,
  -- Características categóricas
  source, -- Se puede usar directamente o en ML.FEATURE_CROSS
  medium,
  channel_grouping,
  city,
  country,
  CAST(product_detail_view AS STRING) AS product_detail_view_str, -- Es 0/1, pero CAST para consistencia en cruces si se usa

  -- Creación de nuevas características a partir de la fecha
  EXTRACT(DAYOFWEEK FROM parsed_date) AS day_of_week,
  EXTRACT(MONTH FROM parsed_date) AS month_of_year,

  -- Ejemplo de cruce de características geográficas y de origen
  ML.FEATURE_CROSS(STRUCT(city, country, source)) AS geo_source_cross,

  -- La etiqueta `add_to_cart` debe estar presente en el output de TRANSFORM
  add_to_cart

  -- Característica opcional a considerar (con precaución):
  -- `will_add_to_cart_later` podría ser una feature, pero podría generar "target leakage"
  -- si estás prediciendo `add_to_cart` en la *misma* sesión y `will_add_to_cart_later` se deriva de ella.
  -- Para este modelo, la excluimos para predecir el comportamiento en la sesión actual sin conocimiento futuro directo.
  -- Si el objetivo fuera diferente (ej. predecir `will_add_to_cart_later`), su manejo sería distinto.
)
OPTIONS(
  model_type = "BOOSTED_TREE_CLASSIFIER", -- O "LOGISTIC_REG"
  data_split_method = "AUTO_SPLIT", -- Necesario ya que no hay `split_col`
  input_label_cols = ["add_to_cart"],
  enable_global_explain = TRUE,
  auto_class_weights = TRUE -- Recomendable si la clase 'add_to_cart = 1' es minoritaria
) AS
SELECT
  -- Selecciona todas las columnas que se usan en TRANSFORM y la etiqueta
  parsed_date, -- Usada en TRANSFORM para extraer day_of_week, month_of_year
  bounces,
  time_on_site,
  page_views,
  source,
  medium,
  channel_grouping,
  is_mobile,
  product_detail_view,
  city,
  country,
  -- full_visitor_id, -- Generalmente no se usa como feature directa
  -- will_add_to_cart_later, -- Excluida como feature por ahora, ver comentario en TRANSFORM
  add_to_cart -- Esta es la etiqueta que el modelo aprenderá a predecir
FROM
  `<TU_NOMBRE>.propensidad_anadir_al_carrito`; -- ¡Importante! Usamos la nueva tabla. No hay `WHERE split_col`.

-- Paso 3.2: [[ Ejercicio para el usuario ]] Evaluar tu nuevo modelo para `add_to_cart`.
-- Escribe la consulta SQL para evaluar el rendimiento de tu modelo `ga_add_to_cart_current_session_model`.
-- Presta atención a métricas como roc_auc, precision, recall para la clase positiva (add_to_cart = 1).
-- TODO: Escribe tu consulta ML.EVALUATE aquí
/*
SELECT
  *
FROM
  ML.EVALUATE(MODEL `tu_dataset.ga_add_to_cart_current_session_model`, -- Reemplaza tu_proyecto y tu_dataset
              STRUCT(0.5 AS threshold)); -- Puedes ajustar el umbral
*/

-- Paso 3.3: [[ Ejercicio para el usuario ]] Realizar predicciones con tu nuevo modelo.
-- Escribe la consulta SQL para predecir `add_to_cart` usando los datos de la tabla `propensidad_anadir_al_carrito`.
-- Como usaste AUTO_SPLIT, BigQuery ML se encarga de usar los datos correctos para predicción si le pasas toda la tabla.
-- Para un escenario real, pasarías datos nuevos que no se usaron en el entrenamiento.
-- Aquí, para probar, puedes seleccionar una muestra de la misma tabla.
-- TODO: Escribe tu consulta ML.PREDICT aquí
/*
SELECT
  *
FROM
  ML.PREDICT(
    MODEL `tu_dataset.ga_add_to_cart_current_session_model`, -- Reemplaza tu_proyecto y tu_dataset
    (
      SELECT
        -- Asegúrate de incluir todas las columnas originales que el modelo espera para la transformación
        parsed_date,
        bounces,
        time_on_site,
        page_views,
        source,
        medium,
        channel_grouping,
        is_mobile,
        product_detail_view,
        city,
        country,
        add_to_cart -- Incluye la etiqueta real para poder comparar las predicciones
        -- No incluyas aquí las columnas generadas por TRANSFORM (ej. time_on_site_bucketed),
        -- solo las columnas de entrada originales de la tabla `propensidad_anadir_al_carrito`
      FROM
        `tu_dataset.propensidad_anadir_al_carrito` -- Reemplaza tu_proyecto y tu_dataset
      -- WHERE ... podrías aplicar algún filtro para simular datos "nuevos" o de un segmento específico.
      LIMIT 1000 -- Ejemplo para limitar la predicción
    ),
    STRUCT(0.5 AS threshold) -- Ajusta el umbral si es necesario
  );
*/


------------------------------------------------------------------------------------------------------------------------
-- SOLUCIONES (referencia)
------------------------------------------------------------------------------------------------------------------------
-- Solucion para el modelo con TRANFORM
CREATE OR REPLACE MODEL `whejna_demo_dia4.ga_propensidad_compra_model_con_transform2` 
TRANSFORM(
  bounces,
  ML.QUANTILE_BUCKETIZE(time_on_site, 10) OVER () AS time_on_site_bucketed,
  ML.QUANTILE_BUCKETIZE(page_views, 10) OVER () AS page_views_bucketed,
  ML.FEATURE_CROSS(STRUCT(source, medium, channel_grouping)) AS origin,
  is_mobile,
  ML.FEATURE_CROSS(STRUCT(CAST(product_detail_view AS STRING) AS product_detail_view, CAST(add_to_cart AS STRING) AS add_to_cart)) AS behaviour,
  will_buy_later
)
OPTIONS(
  -- Especifica el tipo de modelo a crear como Regresión Logística.
  model_type = "LOGISTIC_REG",
  -- Divide automáticamente los datos en conjuntos de entrenamiento y evaluación.
  data_split_method = "AUTO_SPLIT",
  -- Define la columna "will_buy_later" como la variable objetivo para la predicción.
  input_label_cols = ["will_buy_later"],
  -- Habilita la explicabilidad global para el modelo, proporcionando información sobre la importancia de las características.
  enable_global_explain = TRUE
) AS
-- Selecciona las características y la variable objetivo del dataset preparado.
SELECT
  bounces,
  time_on_site,
  page_views,
  source,
  medium,
  channel_grouping,
  is_mobile,
  add_to_cart,
  product_detail_view,
  will_buy_later
-- Utiliza el dataset preparado para el entrenamiento, filtrando por filas con split_col = 'training'.
FROM
  `whejna_demo_dia4.ga_propensidad_compra_ready_for_ml`
WHERE
  split_col = 'training';
