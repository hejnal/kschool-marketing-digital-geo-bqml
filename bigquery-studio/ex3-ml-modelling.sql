-- Instrucciones para ejercicios de modelado de ML con BigQuery SQL

-- 1. Crear un nuevo dataset personal (reemplazar <TU_DATSET_PERSONAL> con tu nombre)
CREATE SCHEMA IF NOT EXISTS `clean-silo-405314.<TU_DATSET_PERSONAL>`
OPTIONS(location = 'US');

-- 1. (Requerido) Crea y evalua un modelo base

-- Esta sección crea o reemplaza un modelo de regresión logística llamado `ga_propensidad_compra_baseline_model` en el dataset `<TU_DATSET_PERSONAL>`
CREATE OR REPLACE MODEL `<TU_DATSET_PERSONAL>.ga_propensidad_compra_baseline_model` OPTIONS(
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
  `<TU_DATSET_PERSONAL>.ga_propensidad_compra_ready_for_ml`
WHERE
  split_col = 'training';

-- Evaluar modelo
-- Esta sección evalúa el rendimiento del modelo entrenado utilizando los datos de validación.
SELECT
  *
FROM
  -- Utiliza la función ML.EVALUATE para evaluar el modelo.
  ML.EVALUATE(MODEL `<TU_DATSET_PERSONAL>.ga_propensidad_compra_baseline_model`,
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
     `<TU_DATSET_PERSONAL>.ga_propensidad_compra_ready_for_ml`
    WHERE
      split_col = 'validation'),
    -- Establece el umbral para la clasificación en 0.5.
    STRUCT(0.5 AS threshold));

-- Predecir resultados
-- Esta sección utiliza el modelo entrenado para predecir la variable objetivo para los datos de prueba.
SELECT
  *
FROM
  -- Utiliza la función ML.PREDICT para realizar predicciones.
  ML.PREDICT(
    MODEL `<TU_DATSET_PERSONAL>.ga_propensidad_compra_baseline_model`,
    -- Selecciona los datos de prueba del dataset preparado.
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
        `<TU_DATSET_PERSONAL>.ga_propensidad_compra_ready_for_ml`
      WHERE
        split_col = 'test'
    ),
    -- Establece el umbral para la clasificación en 0.5.
    STRUCT(0.5 AS threshold)
  );