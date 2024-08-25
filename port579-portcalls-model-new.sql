-- Add total_portcalls_non_container Column
ALTER TABLE `sunny-emblem-423616-k6.data.port579_data`
ADD COLUMN total_portcalls_non_container INT64;

-- Add import_export_ratio Column
ALTER TABLE `sunny-emblem-423616-k6.data.port579_data`
ADD COLUMN import_export_ratio FLOAT64; 

-- Add day_of_week Column
ALTER TABLE `sunny-emblem-423616-k6.data.port579_data`
ADD COLUMN day_of_week INT64; 

-- Create a New Model with New Features
CREATE OR REPLACE MODEL `sunny-emblem-423616-k6.data.port579_portcalls_model_new`
OPTIONS (model_type='linear_reg', input_label_cols=['portcalls']) AS
SELECT
    year,
    month,
    day,
    portid,
    portname,
    country,
    (portcalls_dry_bulk + portcalls_general_cargo + portcalls_roro + portcalls_tanker) AS total_portcalls_non_container,
    SAFE_DIVIDE(import, export) AS import_export_ratio,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    portcalls
  FROM
    `sunny-emblem-423616-k6.data.port579_data`
  WHERE month = 6
   AND year BETWEEN 2019 AND 2023
   AND NOT IS_NAN(SAFE_DIVIDE(import, export));

-- Calculate Features for June 2024
UPDATE
  `sunny-emblem-423616-k6.data.port579_data`
SET
  total_portcalls_non_container = ( portcalls_dry_bulk + portcalls_general_cargo + portcalls_roro + portcalls_tanker ),
  import_export_ratio = SAFE_DIVIDE(import, export),
  day_of_week = MOD(EXTRACT(DAYOFWEEK FROM date) + 5, 7) + 1  -- This expression adjusts the day OF the week TO match the standard ISO 8601 week numbering
WHERE
  month = 6
  AND year = 2024;

-- Make Predictions
SELECT
  *
FROM
  ML.PREDICT(MODEL `sunny-emblem-423616-k6.data.port579_portcalls_model_new`,
    (
    SELECT
      year,
      month,
      day,
      portid,
      portname,
      country,
      total_portcalls_non_container,
      import_export_ratio,
      day_of_week,
      portcalls
    FROM
      `sunny-emblem-423616-k6.data.port579_data`
    WHERE
      month = 6
      AND year = 2024 ) )
