/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_alerts_agg_hour
(
  uuid STRING,
  georss_date TIMESTAMP,
  start_date TIMESTAMP,
  the_geom GEOGRAPHY,
  street STRING,
  type STRING,
  subtype STRING
)
PARTITION BY TIMESTAMP_TRUNC(georss_date, DAY)
OPTIONS(require_partition_filter=true)
