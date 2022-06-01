#standardSQL

--demo baseline model
CREATE OR REPLACE MODEL
  ml_models.demo_baseline_ga_logistic_regression_model OPTIONS( model_type="LOGISTIC_REG",
    data_split_method="AUTO_SPLIT",
    input_label_cols=["will_buy_later"],
    enable_global_explain=TRUE) AS
SELECT
  bounces,
  time_on_site,
  page_views,
  source,
  medium,
  channel_grouping,
  is_mobile,
  add_to_cart,
  artist_detail_view,
  will_buy_later
FROM
  web_analytics_eu.indie_label_events_data_with_bias_with_split
WHERE
  split_col = 'training';

-- predict results
SELECT
  *
FROM
  ML.PREDICT(MODEL ml_models.demo_baseline_logistic_regression_model,
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
  artist_detail_view,
  will_buy_later
FROM
  web_analytics_eu.indie_label_events_data_with_bias_with_split
WHERE
  split_col = 'test'
    ));

-- evaluate the model
CREATE OR REPLACE MODEL
  ml_models.demo_baseline_ga_logistic_regression_model OPTIONS( model_type="LOGISTIC_REG",
    data_split_method="AUTO_SPLIT",
    input_label_cols=["will_buy_later"],
    enable_global_explain=TRUE) AS
SELECT
  bounces,
  time_on_site,
  page_views,
  source,
  medium,
  channel_grouping,
  is_mobile,
  add_to_cart,
  artist_detail_view,
  will_buy_later
FROM
  web_analytics_eu.indie_label_events_data_with_bias_with_split
WHERE
  split_col = 'training';