/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_jams
(
  the_geom GEOGRAPHY,
  country STRING,
  city STRING,
  speed FLOAT64,
  length FLOAT64,
  level INT64,
  delay INT64,
  date TIMESTAMP,
  startnode STRING,
  endnode STRING,
  street STRING,
  roadtype INT64,
  type STRING,
  turntype STRING,
  uuid STRING,
  blockingalert_uuid STRING,
  georss_date TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(georss_date, DAY)
OPTIONS(require_partition_filter=true)
