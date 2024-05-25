-- Create or replace a table named `<dataset_id>.<table_name>` optimized for adding to cart analysis.
CREATE OR REPLACE TABLE `<dataset_id>.<table_name>` AS (
  -- Select all relevant data from the ga_sessions_* table.
  SELECT
    *
  FROM
    -- Subquery to extract and process session-level data.
    (
      SELECT
        -- Parse the date string into a timestamp.
        PARSE_TIMESTAMP("%Y%m%d", date) AS parsed_date,
        -- Extract the full visitor ID.
        fullVisitorId AS full_visitor_id,
        -- Get the number of bounces, handling null values.
        IFNULL(totals.bounces, 0) AS bounces,
        -- Get the time spent on site, handling null values.
        IFNULL(totals.timeOnSite, 0) AS time_on_site,
        -- Extract the number of page views.
        totals.pageviews AS page_views,
        -- Extract the traffic source.
        trafficSource.source,
        -- Extract the traffic medium.
        trafficSource.medium,
        -- Extract the channel grouping.
        channelGrouping AS channel_grouping,
        -- Check if the device is mobile.
        device.isMobile AS is_mobile,
        -- Check if the session contains an add-to-cart event.
        IF (
          -- Subquery to count add-to-cart events within the session.
          (
            SELECT
              SUM(
                -- Count add-to-cart events (action_type = 3).
                IF (
                  eCommerceAction.action_type = '3',
                  1,
                  0
                )
              )
            FROM
              -- Unnest the hits array to access individual events.
              UNNEST(hits)
          ) >= 1,
          -- Set the add_to_cart flag to 1 if there's at least one add-to-cart event.
          1,
          -- Otherwise, set it to 0.
          0
        ) AS add_to_cart,
        -- Check if the session contains a product detail view event.
        IF (
          -- Subquery to count product detail view events within the session.
          (
            SELECT
              SUM(
                -- Count product detail view events (action_type = 2).
                IF (
                  eCommerceAction.action_type = '2',
                  1,
                  0
                )
              )
            FROM
              -- Unnest the hits array to access individual events.
              UNNEST(hits)
          ) >= 1,
          -- Set the product_detail_view flag to 1 if there's at least one product detail view event.
          1,
          -- Otherwise, set it to 0.
          0
        ) AS product_detail_view,
        -- Extract the city, handling null values.
        IFNULL(geoNetwork.city, "") AS city,
        -- Extract the country, handling null values.
        IFNULL(geoNetwork.country, "") AS country
      FROM
        -- Select data from the ga_sessions_* table.
        `bigquery-public-data.google_analytics_sample.ga_sessions_*` s
      -- Filter for new visits only.
      WHERE
        totals.newVisits = 1
        -- Filter for sessions within the specified date range.
        AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    )
    -- Join with a subquery to identify visitors who will add to cart later.
    JOIN (
      SELECT
        -- Extract the full visitor ID.
        fullvisitorid AS full_visitor_id,
        -- Check if the visitor added to cart in any subsequent session.
        IF (
          -- Count sessions where the visitor added to cart.
          COUNTIF(
            -- Check for new visits and add-to-cart events.
            new_visit IS NOT NULL
            AND added_to_cart >= 1
          ) > 0,
          -- Set the will_add_to_cart_later flag to 1 if the visitor added to cart in a subsequent session.
          1,
          -- Otherwise, set it to 0.
          0
        ) AS will_add_to_cart_later
      FROM
        -- Subquery to extract and process session-level data for the join.
        (
          SELECT
            -- Extract the full visitor ID.
            fullvisitorid,
            -- Check if the session is a new visit.
            totals.newVisits AS new_visit,
            -- Check if the session contains an add-to-cart event.
            IF(
              -- Subquery to count add-to-cart events within the session.
              (
                SELECT
                  SUM(
                    -- Count add-to-cart events (action_type = 3).
                    IF (
                      eCommerceAction.action_type = '3',
                      1,
                      0
                    )
                  )
                FROM
                  -- Unnest the hits array to access individual events.
                  UNNEST(hits)
              ) >= 1,
              -- Set the added_to_cart flag to 1 if there's at least one add-to-cart event.
              1,
              -- Otherwise, set it to 0.
              0
            ) AS added_to_cart
          FROM
            -- Select data from the ga_sessions_* table.
            `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        )
      -- Group by full visitor ID to aggregate data across sessions.
      GROUP BY
        fullvisitorid
    ) USING (full_visitor_id)
);