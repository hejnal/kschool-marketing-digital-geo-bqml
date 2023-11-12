### Instructions

1) Change strategy - optimize add to cart action

2) Compare both models

`Hint`:
```
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
        IF((
        SELECT
          SUM(
          IF
            (eCommerceAction.action_type='3',
              1,
              0))
        FROM
          UNNEST(hits)) >=1, 1, 0) AS added_to_cart
      FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`)
    GROUP BY
      fullvisitorid;
```