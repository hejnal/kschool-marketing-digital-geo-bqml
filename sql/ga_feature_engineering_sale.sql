-- Create or replace a table named `<dataset_id>.<table_name>` with optimized data for sales analysis.
CREATE OR REPLACE TABLE `<dataset_id>.<table_name>` AS (
  -- Select all columns from the subquery.
  SELECT
    *
  FROM
    -- Subquery to extract relevant data from the GA sessions table.
    (
      SELECT
        -- Parse the date string into a timestamp format.
        PARSE_TIMESTAMP("%Y%m%d", date) AS parsed_date,
        -- Select the full visitor ID.
        fullVisitorId AS full_visitor_id,
        -- Get the number of bounces, defaulting to 0 if null.
        IFNULL(totals.bounces, 0) AS bounces,
        -- Get the time spent on site, defaulting to 0 if null.
        IFNULL(totals.timeOnSite, 0) AS time_on_site,
        -- Select the number of page views.
        totals.pageviews AS page_views,
        -- Select the traffic source.
        trafficSource.source,
        -- Select the traffic medium.
        trafficSource.medium,
        -- Select the channel grouping.
        channelGrouping AS channel_grouping,
        -- Check if the device is mobile.
        device.isMobile AS is_mobile,
        -- Check if the user added items to cart.
        IF (
          -- Subquery to count the number of add-to-cart actions.
          (
            SELECT
              SUM(
                -- Count add-to-cart actions (action_type = 3).
                IF (
                  eCommerceAction.action_type = '3',
                  1,
                  0
                )
              )
            FROM
              -- Unnest the hits array to access individual actions.
              UNNEST(hits)
          ) >= 1,
          -- Set to 1 if there's at least one add-to-cart action, otherwise 0.
          1,
          0
        ) AS add_to_cart,
        -- Check if the user viewed product details.
        IF (
          -- Subquery to count the number of product detail views.
          (
            SELECT
              SUM(
                -- Count product detail views (action_type = 2).
                IF (
                  eCommerceAction.action_type = '2',
                  1,
                  0
                )
              )
            FROM
              -- Unnest the hits array to access individual actions.
              UNNEST(hits)
          ) >= 1,
          -- Set to 1 if there's at least one product detail view, otherwise 0.
          1,
          0
        ) AS product_detail_view,
        -- Get the city, defaulting to an empty string if null.
        IFNULL(geoNetwork.city, "") AS city,
        -- Get the country, defaulting to an empty string if null.
        IFNULL(geoNetwork.country, "") AS country
      FROM
        -- Select data from the GA sessions table for the specified date range.
        `bigquery-public-data.google_analytics_sample.ga_sessions_*` s
      WHERE
        -- Filter for new visits only.
        totals.newVisits = 1
        -- Filter for sessions between August 1, 2016 and August 1, 2017.
        AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    )
    -- Join the subquery with another subquery to identify users who will buy later.
    JOIN (
      SELECT
        -- Select the full visitor ID.
        fullVisitorId AS full_visitor_id,
        -- Check if the user made a purchase later.
        IF (
          -- Count the number of sessions with transactions and no new visits.
          COUNTIF(
            totals.transactions > 0
            AND totals.newVisits IS NULL
          ) > 0,
          -- Set to 1 if there's at least one such session, otherwise 0.
          1,
          0
        ) AS will_buy_later
      FROM
        -- Select data from the GA sessions table.
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      GROUP BY
        -- Group by full visitor ID.
        fullVisitorId
    ) USING (full_visitor_id)
);