### Instructions

1) Make a split between training, evaluation, test

2) Create a base model with few arguments (and a simple algorithm)

3) Evaluate the model and plan the next iteration

4) Apply model changes (algorithm, parameters, features, etc)

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