/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_irrgs
(
  the_geom GEOGRAPHY,
  country STRING,
  city STRING,
  speed FLOAT64,
  regularspeed FLOAT64,
  length FLOAT64,
  jamlevel INT64,
  severity INT64,
  highway BOOL,
  trend INT64,
  seconds INT64,
  delayseconds INT64,
  detectiondate TIMESTAMP,
  updatedate TIMESTAMP,
  startnode STRING,
  endnode STRING,
  street STRING,
  ncomments INT64,
  nimages INT64,
  nthumbsup INT64,
  id INT64,
  type STRING,
  alertscount INT64,
  alerts_uuid ARRAY<STRING>,
  georss_date TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(georss_date, DAY)
OPTIONS(require_partition_filter=true)
