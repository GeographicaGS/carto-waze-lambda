WITH id_partitioned_irrgs_with_alerts AS (
  SELECT
    id,
    alerts_uuid,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY length DESC) AS r_n
  FROM
    `DATASET_NAME.PREFIX_waze_data_irrgs`
  WHERE
    DATE(georss_date) = (current_date - 1)
    AND alerts_uuid IS NOT NULL
),
longest_unique_irrgs_with_alerts AS (
  SELECT
    id,
    alerts_uuid
  FROM
    id_partitioned_irrgs_with_alerts
  WHERE
    r_n = 1
),
id_partitioned_irrgs AS (
  SELECT
    id,
    the_geom,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY length DESC) AS r_n
  FROM
    `DATASET_NAME.PREFIX_waze_data_irrgs`
  WHERE
    DATE(georss_date) = (current_date - 1)
),
longest_unique_irrgs AS (
  SELECT
    id,
    the_geom
  FROM
    id_partitioned_irrgs
  WHERE
    r_n = 1
),
related_alerts AS (
  SELECT
    i.id,
    a.type,
    a.subtype
  FROM
    `DATASET_NAME.PREFIX_waze_data_alerts` a
    JOIN longest_unique_irrgs_with_alerts i ON a.uuid IN unnest(i.alerts_uuid)
  WHERE
    DATE(a.georss_date) = (current_date - 1)
    AND a.type NOT LIKE 'JAM'
  GROUP BY
    i.id,
    a.type,
    a.subtype
),
irrgs_streets AS (
  SELECT
    i.id,
    s.ntram
  FROM
    longest_unique_irrgs i
    JOIN `DATASET_NAME.PREFIX_streets` s ON ST_INTERSECTS (i.the_geom, s.the_geom)
),
irrgs_with_level AS (
  SELECT
    id,
    georss_date,
    CASE
      WHEN UPPER(type) = 'SMALL' THEN
        2
      WHEN UPPER(type) = 'MEDIUM' THEN
        3
      WHEN UPPER(type) = 'LARGE' THEN
        4
      WHEN UPPER(type) = 'HUGE' THEN
        5
      ELSE
        0
    END AS level,
    speed,
    length,
    detectiondate,
    regularspeed
  FROM
  `DATASET_NAME.PREFIX_waze_data_irrgs`
  WHERE
    DATE(georss_date) = (current_date - 1)
),
irrgs_by_hour AS (
  SELECT
    id,
    timestamp_trunc(georss_date, HOUR) AS georss_date,
    timestamp_diff(max(georss_date), min(detectiondate), SECOND) as duration_seconds,
    AVG(level) AS avg_level,
    ROUND(AVG(speed), 2) AS avg_speed,
    ROUND(AVG(length), 2) AS avg_length,
    AVG(regularspeed) AS regular_speed
  FROM
    irrgs_with_level
  GROUP BY
    id,
    timestamp_trunc(georss_date, HOUR)
)
SELECT
  ih.georss_date,
  js.ntram,
  MAX(ih.duration_seconds) AS duration_seconds,
  CAST(ROUND(AVG(ih.avg_level)) AS INT64) AS avg_level,
  ROUND(AVG(ih.avg_speed), 2) AS avg_speed,
  ROUND(AVG(ih.avg_length), 2) AS avg_length,
  ARRAY_AGG(DISTINCT ra.type IGNORE NULLS) AS alert_types,
  ARRAY_AGG(DISTINCT ra.subtype IGNORE NULLS) AS alert_subtypes,
  CASE
    WHEN AVG(regular_speed) < 70 THEN
      1
    ELSE
      6
  END AS road_type
FROM
  irrgs_by_hour ih
  JOIN irrgs_streets js ON ih.id = js.id
  LEFT JOIN related_alerts ra ON ih.id = ra.id
GROUP BY
  ih.georss_date,
  js.ntram
