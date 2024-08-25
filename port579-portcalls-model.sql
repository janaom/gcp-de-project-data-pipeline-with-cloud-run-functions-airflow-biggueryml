-- Create a model
CREATE OR REPLACE MODEL `project-id.dataset.port579_portcalls_model`
OPTIONS(model_type='linear_reg', input_label_cols=['portcalls']) AS
SELECT
  year,
  month,
  day,
  portid,
  portname,
  country,
  portcalls
FROM
  `project-id.dataset.port579_data`
WHERE
  month = 6 AND year >= 2019 AND year <= 2023

-- Make predictions
SELECT
  year,
  month,
  day,
  portid,
  portname,
  country,
  portcalls,
  predicted_portcalls
FROM
  ML.PREDICT(MODEL `project-id.dataset.port579_portcalls_model`,
    (
    SELECT
      year,
      month,
      day,
      portid,
      portname,
      country,
      portcalls
    FROM
      `project-id.dataset.port579_data`
    WHERE
      month = 6
      AND year = 2024 ) )
