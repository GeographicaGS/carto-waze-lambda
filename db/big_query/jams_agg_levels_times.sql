/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_jams_agg_levels_times
(
  id STRING,
  ntram INT64,
  level INT64,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  avg_speed FLOAT64
)
PARTITION BY TIMESTAMP_TRUNC(start_ts, DAY)
OPTIONS(require_partition_filter=true)
