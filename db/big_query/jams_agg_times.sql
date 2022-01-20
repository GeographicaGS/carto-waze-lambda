/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_jams_agg_times
(
  ntram INT64,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(start_ts, DAY)
OPTIONS(require_partition_filter=true)
