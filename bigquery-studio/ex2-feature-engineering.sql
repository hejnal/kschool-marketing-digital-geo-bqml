-- Instrucciones para ejercicios de ingeniería de características con BigQuery SQL

/*
Objetivo: Investigar el dataset público `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
para seleccionar las características más adecuadas para un modelo de propensidad.

Este script incluye:
1. Creación de un nuevo dataset personal
2. Consultas básicas para explorar los datos
3. Ejercicios para extraer características relevantes usando operadores como UNNEST(), IFNULL(), COUNTIF()

HINT: Para consultar datos de un día específico, usa el operador _TABLE_SUFFIX = '20160801'
Para consultar datos de múltiples días, usa _TABLE_SUFFIX BETWEEN '20160801' AND '20160831'
El operador * en ga_sessions_* permite consultar todas las tablas que coincidan con ese patrón.
*/

-- 1. Crear un nuevo dataset personal (reemplazar <DEMO_CLAUDIAL> con tu nombre)
CREATE SCHEMA IF NOT EXISTS `clean-silo-405314.DEMO_CLAUDIA`
OPTIONS(location = 'US');

-- 2. Consultas básicas para explorar el dataset

-- Ejercicio 1: ¿De qué ciudad de EEUU se ha originado más tráfico en GA durante el día 20160801?
-- Tu código aquí
SELECT *
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` 
LIMIT 100;

SELECT geoNetwork.city, COUNT(*)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` 
WHERE geoNetwork.country='United States'
AND geoNetwork.city <> 'not available in demo dataset'
GROUP BY 1 
ORDER BY 2 DESC
LIMIT 1;

-- Ejercicio 2: ¿Qué página de producto ha sido la más visitada (durante el día 20160801) globalmente?
-- Tu código aquí

SELECT geoNetwork.city, COUNT(*)
-- FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` 

GROUP BY 1 
ORDER BY 2 DESC
LIMIT 1;


-- 3. Ejercicios para extraer características relevantes (Opcional)

-- Ejercicio 3: Extraer características básicas de sesiones
-- Placeholder: Crear una tabla con características básicas de usuario
-- Tu código aquí

-- Ejercicio 4: Analizar comportamiento de compra
-- Placeholder: Identificar usuarios que han realizado compras
-- Tu código aquí

-- Ejercicio 5: Crear características para modelo de propensidad
-- Placeholder: Crear tabla con características para predecir propensidad de compra
-- Tu código aquí


-- 4. (Requerido) Prepara los datos para modelar (ejectuar tal cual, creando datos para crear el primer modelo ML)
-- Crea o reemplaza una tabla llamada `<TU_DATSET_PERSONAL>.ga_propensidad_compra` con datos optimizados para análisis de ventas.
CREATE OR REPLACE TABLE `<TU_DATSET_PERSONAL>.ga_propensidad_compra` AS (
  -- Selecciona todas las columnas de la subconsulta.
  SELECT
    *
  FROM
    -- Subconsulta para extraer datos relevantes de la tabla de sesiones de GA.
    (
      SELECT
        -- Analiza la cadena de fecha a un formato de marca de tiempo.
        PARSE_TIMESTAMP("%Y%m%d", date) AS parsed_date,
        -- Selecciona el ID completo del visitante.
        fullVisitorId AS full_visitor_id,
        -- Obtiene el número de rebotes, por defecto 0 si es nulo.
        IFNULL(totals.bounces, 0) AS bounces,
        -- Obtiene el tiempo pasado en el sitio, por defecto 0 si es nulo.
        IFNULL(totals.timeOnSite, 0) AS time_on_site,
        -- Selecciona el número de páginas vistas.
        totals.pageviews AS page_views,
        -- Selecciona la fuente de tráfico.
        trafficSource.source,
        -- Selecciona el medio de tráfico.
        trafficSource.medium,
        -- Selecciona la agrupación de canales.
        channelGrouping AS channel_grouping,
        -- Verifica si el dispositivo es móvil.
        device.isMobile AS is_mobile,
        -- Verifica si el usuario añadió artículos al carrito.
        IF (
          -- Subconsulta para contar el número de acciones de añadir al carrito.
          (
            SELECT
              SUM(
                -- Cuenta las acciones de añadir al carrito (action_type = 3).
                IF (
                  eCommerceAction.action_type = '3',
                  1,
                  0
                )
              )
            FROM
              -- Desanida el array de hits para acceder a las acciones individuales.
              UNNEST(hits)
          ) >= 1,
          -- Establece a 1 si hay al menos una acción de añadir al carrito, de lo contrario 0.
          1,
          0
        ) AS add_to_cart,
        -- Verifica si el usuario vio detalles del producto.
        IF (
          -- Subconsulta para contar el número de vistas de detalles de producto.
          (
            SELECT
              SUM(
                -- Cuenta las vistas de detalles de producto (action_type = 2).
                IF (
                  eCommerceAction.action_type = '2',
                  1,
                  0
                )
              )
            FROM
              -- Desanida el array de hits para acceder a las acciones individuales.
              UNNEST(hits)
          ) >= 1,
          -- Establece a 1 si hay al menos una vista de detalle de producto, de lo contrario 0.
          1,
          0
        ) AS product_detail_view,
        -- Obtiene la ciudad, por defecto una cadena vacía si es nulo.
        IFNULL(geoNetwork.city, "") AS city,
        -- Obtiene el país, por defecto una cadena vacía si es nulo.
        IFNULL(geoNetwork.country, "") AS country
      FROM
        -- Selecciona datos de la tabla de sesiones de GA para el rango de fechas especificado.
        `bigquery-public-data.google_analytics_sample.ga_sessions_*` s
      WHERE
        -- Filtra solo para nuevas visitas.
        totals.newVisits = 1
        -- Filtra para sesiones entre el 1 de agosto de 2016 y el 1 de agosto de 2017.
        AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    )
    -- Une la subconsulta con otra subconsulta para identificar usuarios que comprarán más tarde.
    JOIN (
      SELECT
        -- Selecciona el ID completo del visitante.
        fullVisitorId AS full_visitor_id,
        -- Verifica si el usuario realizó una compra más tarde.
        IF (
          -- Cuenta el número de sesiones con transacciones y sin nuevas visitas.
          COUNTIF(
            totals.transactions > 0
            AND totals.newVisits IS NULL
          ) > 0,
          -- Establece a 1 si hay al menos una sesión así, de lo contrario 0.
          1,
          0
        ) AS will_buy_later
      FROM
        -- Selecciona datos de la tabla de sesiones de GA.
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      GROUP BY
        -- Agrupa por ID completo del visitante.
        fullVisitorId
    ) USING (full_visitor_id)
);


-- 5. (Requerido) Realiza un split de datos para entrenar y evaluar
-- Crea o reemplaza una tabla llamada `<TU_DATSET_PERSONAL>.ga_propensidad_compra_ready_for_ml`
CREATE OR REPLACE TABLE `<TU_DATSET_PERSONAL>.ga_propensidad_compra_ready_for_ml` AS (
  -- Selecciona todas las columnas de la tabla original y añade una nueva columna llamada 'split_col'
  SELECT
    *,
    -- Calcula la columna de división basada en el hash de las características crudas
    CASE
      -- Calcula el módulo del valor absoluto del hash de las características crudas (convertidas a cadena JSON) por 10
      (
        MOD(
          ABS(
            -- Calcula el hash de las características crudas usando la función FARM_FINGERPRINT
            FARM_FINGERPRINT(TO_JSON_STRING(raw_features))
          ),
          10
        )
      )
      -- Si el módulo es 9, asigna 'test' a la columna split_col
      WHEN 9 THEN 'test'
      -- Si el módulo es 8, asigna 'validation' a la columna split_col
      WHEN 8 THEN 'validation'
      -- De lo contrario, asigna 'training' a la columna split_col
      ELSE 'training'
    END AS split_col
  -- Selecciona datos de la tabla original
  FROM
    `<TU_DATSET_PERSONAL>.ga_propensidad_compra` raw_features
);

------------------------------------------------------------------------------------------------------------------------
-- SOLUCIONES (referencia)
------------------------------------------------------------------------------------------------------------------------

-- Solución Ejercicio 1: ¿De qué ciudad de EEUU se ha originado más tráfico en GA durante el día 20160801?
SELECT 
  geoNetwork.city AS city,
  COUNT(*) AS total_sessions
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE 
  _TABLE_SUFFIX = '20160801'
  AND geoNetwork.country = 'United States'
  AND geoNetwork.city != 'not available in demo dataset'
GROUP BY 
  city
ORDER BY 
  total_sessions DESC
LIMIT 10;

-- Solución Ejercicio 2: ¿Qué página de producto ha sido la más visitada (durante el día 20160801) globalmente?
SELECT 
  hits.page.pagePath AS page,
  COUNT(*) AS total_visits
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE 
  _TABLE_SUFFIX = '20160801'
  AND hits.type = 'PAGE'
  AND hits.page.pagePath NOT IN ('/home', '/basket.html')
GROUP BY 
  page
ORDER BY 
  total_visits DESC
LIMIT 10;

-- Solución Ejercicio 3: Extraer características básicas de sesiones
CREATE OR REPLACE TABLE `clean-silo-405314.<TU_DATSET_PERSONAL>.basic_features` AS (
  SELECT
    fullVisitorId,
    visitId,
    date,
    totals.bounces,
    totals.timeOnSite,
    device.deviceCategory,
    geoNetwork.country,
    trafficSource.medium
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX = '20160801'
);

-- Solución Ejercicio 4: Analizar comportamiento de compra
CREATE OR REPLACE TABLE `clean-silo-405314.<TU_DATSET_PERSONAL>.purchase_behavior` AS (
  SELECT
    fullVisitorId,
    COUNTIF(hits.eCommerceAction.action_type = '6') > 0 AS made_purchase,
    SUM(IFNULL(hits.transaction.transactionRevenue, 0))/1000000 AS total_revenue
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX = '20160801'
  GROUP BY
    fullVisitorId
);

-- Solución Ejercicio 5: Crear características para modelo de propensidad
CREATE OR REPLACE TABLE `clean-silo-405314.<TU_DATSET_PERSONAL>.propensity_model` AS (
  SELECT
    s.fullVisitorId,
    MAX(s.visitId) AS last_visit,
    COUNT(DISTINCT s.visitId) AS total_visits,
    SUM(IFNULL(s.totals.pageviews, 0)) AS total_pageviews,
    SUM(IFNULL(s.totals.timeOnSite, 0)) AS total_time_on_site,
    COUNTIF(s.totals.bounces = 1) AS total_bounces,
    COUNTIF(h.eCommerceAction.action_type = '2') AS product_views,
    COUNTIF(h.eCommerceAction.action_type = '3') AS add_to_cart,
    COUNTIF(h.eCommerceAction.action_type = '6') AS purchases_made,
    CASE WHEN COUNTIF(h.eCommerceAction.action_type = '6') > 0 THEN 1 ELSE 0 END AS is_buyer
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*` s,
    UNNEST(hits) AS h
  WHERE
    _TABLE_SUFFIX = '20160801'
  GROUP BY
    s.fullVisitorId
);
