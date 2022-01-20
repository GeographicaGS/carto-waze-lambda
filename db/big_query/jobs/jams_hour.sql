WITH id_partitioned_jams AS (
  SELECT
    uuid,
    the_geom,
    blockingalert_uuid,
    ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY length DESC) AS r_n
  FROM
    `DATASET_NAME.PREFIX_waze_data_jams`
  WHERE
    DATE(georss_date) = (current_date - 1)
),
longest_unique_jams AS (
  SELECT
    uuid,
    the_geom,
    blockingalert_uuid
  FROM
    id_partitioned_jams
  WHERE
    r_n = 1
),
related_alerts AS (
  SELECT
    j.uuid,
    a.type,
    a.subtype
  FROM
    `DATASET_NAME.PREFIX_waze_data_alerts` a
    JOIN longest_unique_jams j ON a.uuid = j.blockingalert_uuid
  WHERE
    DATE(a.georss_date) = (current_date - 1)
    AND a.type NOT LIKE 'JAM'
  GROUP BY
    j.uuid,
    a.type,
    a.subtype
),
jams_streets AS (
  SELECT
    j.uuid,
    s.ntram
  FROM
    longest_unique_jams j
    JOIN `DATASET_NAME.PREFIX_streets` s ON ST_INTERSECTS (j.the_geom, s.the_geom)
),
jams_by_hour AS (
  SELECT
    uuid,
    timestamp_trunc(georss_date, HOUR) AS georss_date,
    timestamp_diff(max(georss_date), min(date), SECOND) as duration_seconds,
    cast(round(avg(level)) as INT64) AS avg_level,
    round(avg(speed), 2) AS avg_speed,
    round(avg(length), 2) AS avg_length,
    min(date) AS start_date,
    max(roadType) as road_type
  FROM
    `DATASET_NAME.PREFIX_waze_data_jams`
  WHERE
    DATE(georss_date) = (current_date - 1)
  GROUP BY
    uuid,
    timestamp_trunc(georss_date, HOUR)
)

SELECT
  jh.georss_date,
  js.ntram,
  MAX(jh.duration_seconds) AS duration_seconds,
  CAST(ROUND(AVG(jh.avg_level)) AS INT64) AS avg_level,
  ROUND(AVG(jh.avg_speed), 2) AS avg_speed,
  ROUND(AVG(jh.avg_length), 2) AS avg_length,
  ARRAY_AGG(DISTINCT ra.type IGNORE NULLS) AS alert_types,
  ARRAY_AGG(DISTINCT ra.subtype IGNORE NULLS) AS alert_subtypes,
  MAX(road_type) AS road_type
FROM
  jams_by_hour jh
  JOIN jams_streets js ON jh.uuid = js.uuid
  LEFT JOIN related_alerts ra ON jh.uuid = ra.uuid
GROUP BY
  jh.georss_date,
  js.ntram
