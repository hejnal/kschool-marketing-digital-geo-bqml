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

-- 1. Crear un nuevo dataset personal (reemplazar <YOUR_NAME> con tu nombre)
CREATE SCHEMA IF NOT EXISTS `clean-silo-405314.<YOUR_NAME>_dataset`
OPTIONS(location = 'US');

-- 2. Consultas básicas para explorar el dataset

-- Pregunta: ¿De qué ciudad de EEUU se ha originado más tráfico en GA durante el día 20160801?
-- Tu código aquí

-- Pregunta: ¿Qué página de producto ha sido la más visitada (durante el día 20160801) globalmente?
-- Tu código aquí

-- 3. Ejercicios para extraer características relevantes

-- Ejercicio 1: Extraer características básicas de sesiones
-- Placeholder: Crear una tabla con características básicas de usuario
-- Tu código aquí

-- Ejercicio 2: Analizar comportamiento de compra
-- Placeholder: Identificar usuarios que han realizado compras
-- Tu código aquí

-- Ejercicio 3: Crear características para modelo de propensidad
-- Placeholder: Crear tabla con características para predecir propensidad de compra
-- Tu código aquí

-- SOLUCIONES (referencia)

-- Solución: ¿De qué ciudad de EEUU se ha originado más tráfico en GA durante el día 20160801?
SELECT 
  geoNetwork.city AS city,
  COUNT(*) AS total_sessions
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE 
  _TABLE_SUFFIX = '20160801'
  AND geoNetwork.country = 'United States'
GROUP BY 
  city
ORDER BY 
  total_sessions DESC
LIMIT 10;

-- Solución: ¿Qué página de producto ha sido la más visitada (durante el día 20160801) globalmente?
SELECT 
  hits.page.pagePath AS page,
  COUNT(*) AS total_visits
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE 
  _TABLE_SUFFIX = '20160801'
  AND hits.type = 'PAGE'
GROUP BY 
  page
ORDER BY 
  total_visits DESC
LIMIT 10;

-- Solución Ejercicio 1: Extraer características básicas de sesiones
CREATE OR REPLACE TABLE `clean-silo-405314.<YOUR_dataset.basic_features` AS (
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

-- Solución Ejercicio 2: Analizar comportamiento de compra
CREATE OR REPLACE TABLE `clean-silo-405314.<YOUR_dataset.purchase_behavior` AS (
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

-- Solución Ejercicio 3: Crear características para modelo de propensidad
CREATE OR REPLACE TABLE `clean-silo-405314.<YOUR_dataset.propensity_model` AS (
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
