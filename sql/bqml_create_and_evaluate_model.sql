#standardSQL
--demo baseline model
-- This section creates or replaces a logistic regression model named `<model_name>` in the dataset `<model_dataset>`.
CREATE
OR REPLACE MODEL `<model_dataset>.<model_name>` OPTIONS(
  -- Specifies the type of model to be created as Logistic Regression.
  model_type = "LOGISTIC_REG",
  -- Automatically splits the data into training and evaluation sets.
  data_split_method = "AUTO_SPLIT",
  -- Defines the column "will_buy_later" as the target variable for prediction.
  input_label_cols = ["will_buy_later"],
  -- Enables global explainability for the model, providing insights into feature importance.
  enable_global_explain = TRUE,
  -- Automatically assigns weights to classes based on their frequency in the data.
  auto_class_weights = TRUE
) AS
-- Selects the features and target variable from the prepared dataset.
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
-- Uses the prepared dataset for training, filtering for rows with split_col = 'training'.
FROM
  `<curated_dataset>.<table_name>_ready_for_ml`
WHERE
  split_col = 'training';

-- evaluate model
-- This section evaluates the performance of the trained model using the validation data.
SELECT
  *
FROM
  -- Uses the ML.EVALUATE function to evaluate the model.
  ML.EVALUATE(MODEL `<model_dataset>.<model_name>`,
    -- Selects the validation data from the prepared dataset.
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
    -- Sets the threshold for classification at 0.5.
    STRUCT(0.5 AS threshold));

-- predict results
-- This section uses the trained model to predict the target variable for the test data.
SELECT
  *
FROM
  -- Uses the ML.PREDICT function to make predictions.
  ML.PREDICT(
    MODEL `<model_dataset>.<model_name>`,
    -- Selects the test data from the prepared dataset.
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
    ),
    -- Sets the threshold for classification at 0.5.
    STRUCT(0.5 AS threshold)
  );