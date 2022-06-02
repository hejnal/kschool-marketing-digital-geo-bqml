
-- optimised for adding to cart
CREATE OR REPLACE TABLE
  web_analytics.ga_classic_events_data_optimised_for_adding_to_cart AS (
  SELECT
    *
  FROM (
    SELECT
      PARSE_TIMESTAMP("%Y%m%d", date) AS parsed_date,
      fullVisitorId,
      IFNULL(totals.bounces,
        0) AS bounces,
      IFNULL(totals.timeOnSite,
        0) AS time_on_site,
      totals.pageviews AS pageviews,
      trafficSource.source,
      trafficSource.medium,
      channelGrouping,
      device.isMobile,
    IF
      ((
        SELECT
          SUM(
          IF
            (eCommerceAction.action_type='3',
              1,
              0))
        FROM
          UNNEST(hits))>=1,
        1,
        0) AS add_to_cart,
    IF
      ((
        SELECT
          SUM(
          IF
            (eCommerceAction.action_type='2',
              1,
              0))
        FROM
          UNNEST(hits))>=1,
        1,
        0) AS product_detail_view,
      IFNULL(geoNetwork.city,
        "") AS city,
      IFNULL(geoNetwork.country,
        "") AS country
    FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*` s
    WHERE
      totals.newVisits = 1
      AND _TABLE_SUFFIX BETWEEN '20160801'
      AND '20170801')
  JOIN (
    SELECT
      fullvisitorid,
    IF
      (COUNTIF(new_visit IS NOT NULL
          AND added_to_cart >=1) > 0,
        1,
        0) AS will_add_to_cart_later
    FROM (
      SELECT
        fullvisitorid,
        totals.newVisits AS new_visit,
        (
        SELECT
          SUM(
          IF
            (eCommerceAction.action_type='3',
              1,
              0))
        FROM
          UNNEST(hits)) AS added_to_cart
      FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`)
    GROUP BY
      fullvisitorid)
  USING
    (fullVisitorId));