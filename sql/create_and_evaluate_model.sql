#standardSQL
--demo baseline model
CREATE
OR REPLACE MODEL ml_models.demo_baseline_ga_logistic_regression_model OPTIONS(
  model_type = "LOGISTIC_REG",
  data_split_method = "AUTO_SPLIT",
  input_label_cols = ["will_buy_later"],
  enable_global_explain = TRUE,
  auto_class_weights = TRUE
) AS
SELECT
  bounces,
  time_on_site,
  pageviews,
  source,
  medium,
  channelGrouping,
  isMobile,
  add_to_cart,
  product_detail_view,
  will_buy_later
FROM
  `<dataset_id>.<table_name>_ready_for_ml`
WHERE
  split_col = 'training';

-- predict results
SELECT
  *
FROM
  ML.PREDICT(
    MODEL ml_models.demo_baseline_logistic_regression_model,
    (
      SELECT
        bounces,
        time_on_site,
        pageviews,
        source,
        medium,
        channelGrouping,
        isMobile,
        add_to_cart,
        product_detail_view,
        will_buy_later
      FROM
        `<dataset_id>.<table_name>_ready_for_ml`
      WHERE
        split_col = 'test'
    )
  );

-- evaluate the model