/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_jams_agg_hour
(
  georss_date TIMESTAMP,
  ntram INT64,
  avg_level INT64,
  avg_speed FLOAT64,
  avg_length FLOAT64,
  duration_seconds INT64,
  alert_types ARRAY<STRING>,
  alert_subtypes ARRAY<STRING>,
  road_type INT64
)
PARTITION BY TIMESTAMP_TRUNC(georss_date, DAY)
OPTIONS(require_partition_filter=true)
