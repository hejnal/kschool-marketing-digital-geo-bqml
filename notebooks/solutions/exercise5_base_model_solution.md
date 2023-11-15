### Instructions

1) Create a analytics feature table (output from the previous exercise) use script: [ga_feature_engineering.sql](../../../sql/ga_feature_engineering.sql) Make a split between training, evaluation, test with a scritp: [create_input_data_split.sql](../../../sql/create_input_data_split.sql).

2) Create a base model with few arguments (and a simple algorithm), script [create_and_evaluate_model.sql](../../../sql/create_and_evaluate_model.sql)

3) Evaluate the model and plan the next iteration

4) Apply model changes (algorithm, parameters, features, etc)
e.g. change the type to XGBoost: MODEL_TYPE = 'BOOSTED_TREE_CLASSIFIER'

5) Infer the results of a test set (with a threshold)

```
SELECT
  *
FROM
  ML.PREDICT(MODEL `mydataset.mymodel`,
    (
    SELECT
      custom_label,
      column1,
      column2
    FROM
      `mydataset.mytable`),
    STRUCT(0.55 AS threshold))
```