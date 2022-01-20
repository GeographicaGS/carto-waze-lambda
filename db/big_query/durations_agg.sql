/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_durations_agg
(
  uuid STRING,
  georss_date TIMESTAMP,
  duration_seconds INT64
)
PARTITION BY TIMESTAMP_TRUNC(georss_date, DAY)
OPTIONS(require_partition_filter=true)
