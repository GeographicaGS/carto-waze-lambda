/* Substitute DATASET_NAME & PREFIX with the appropiate values */

CREATE TABLE IF NOT EXISTS
DATASET_NAME.PREFIX_waze_data_alerts
(
  the_geom GEOGRAPHY,
  country STRING,
  city STRING,
  reportdescription STRING,
  confidence INT64,
  reportrating INT64,
  reliability INT64,
  date TIMESTAMP,
  street STRING,
  roadtype INT64,
  magvar INT64,
  nthumbsup INT64,
  type STRING,
  subtype STRING,
  uuid STRING,
  jam_uuid STRING,
  georss_date TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(georss_date, DAY)
OPTIONS(require_partition_filter=true)
