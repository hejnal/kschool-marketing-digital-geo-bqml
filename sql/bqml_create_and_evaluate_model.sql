#standardSQL
--demo baseline model
CREATE
OR REPLACE MODEL `<model_dataset>.<model_name>` OPTIONS(
  model_type = "LOGISTIC_REG",
  data_split_method = "AUTO_SPLIT",
  input_label_cols = ["will_buy_later"],
  enable_global_explain = TRUE,
  auto_class_weights = TRUE
) AS
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
  `<curated_dataset>.<table_name>_ready_for_ml`
WHERE
  split_col = 'training';

-- evaluate model
SELECT
  *
FROM
  ML.EVALUATE(MODEL `<model_dataset>.<model_name>`,
    (
    SELECT
      bounces,
      time_on_site,
      page_views,
      SOURCE,
      medium,
      channel_grouping,
      is_mobile,
      add_to_cart,
      product_detail_view,
      will_buy_later
    FROM
     `<curated_dataset>.<table_name>_ready_for_ml`
    WHERE
      split_col = 'validation'),
    STRUCT(0.5 AS threshold));

-- predict results
SELECT
  *
FROM
  ML.PREDICT(
    MODEL `<model_dataset>.<model_name>`,
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
        `<curated_dataset>.<table_name>_ready_for_ml`
      WHERE
        split_col = 'test'
    ), STRUCT(0.5 AS threshold)
  );