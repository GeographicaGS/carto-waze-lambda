WITH id_partitioned_irrgs AS (
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
), irrgs_streets AS (
  SELECT
    i.id,
    s.ntram
  FROM
    longest_unique_irrgs i
    JOIN `DATASET_NAME.PREFIX_streets` s ON ST_INTERSECTS (i.the_geom, s.the_geom)
), data_by_id_and_groups AS (
  SELECT
    id,
    type,
    georss_date,
    LAG(georss_date) OVER (PARTITION BY id, type ORDER BY georss_date) AS prev_ts_group,
    AVG(speed) OVER (PARTITION BY id, type) AS avg_speed,
    MIN(georss_date) OVER (PARTITION BY id) AS first_georss_ts,
    MIN(detectiondate) OVER (PARTITION BY id) AS first_ts,
    MAX(georss_date) OVER (PARTITION BY id) AS last_ts
  FROM
    `DATASET_NAME.PREFIX_waze_data_irrgs`
  WHERE
    DATE(georss_date) = (CURRENT_DATE - 1)
  ORDER BY
    georss_date ASC
), times_by_id_and_groups AS (
SELECT
  id,
  type,
  georss_date,
  avg_speed,
  LEAD(georss_date) OVER (PARTITION BY id ORDER BY georss_date) AS end_ts,
  first_georss_ts,
  first_ts,
  first_ts - first_georss_ts AS start_offset,
  last_ts
FROM
  data_by_id_and_groups
WHERE
  prev_ts_group IS NULL
  OR timestamp_diff (georss_date, prev_ts_group, minute) > 2
)

SELECT
  CAST(tig.id AS STRING) AS id,
  irs.ntram,
  CASE
    WHEN UPPER(tig.type) = 'SMALL' THEN
      2
    WHEN UPPER(tig.type) = 'MEDIUM' THEN
      3
    WHEN UPPER(tig.type) = 'LARGE' THEN
      4
    WHEN UPPER(tig.type) = 'HUGE' THEN
      5
    ELSE
      0
  END AS level,
  CASE
    WHEN tig.georss_date = tig.first_georss_ts THEN
      tig.first_ts
    ELSE
      CASE
        WHEN start_offset > INTERVAL 0 SECOND THEN
          tig.georss_date + start_offset
        ELSE
          tig.georss_date
      END
  END AS start_ts,
  CASE
    WHEN tig.end_ts IS NULL THEN
      CASE
        WHEN start_offset > INTERVAL 0 SECOND THEN
          tig.georss_date + INTERVAL 2 MINUTE + start_offset
        ELSE
          tig.georss_date + INTERVAL 2 MINUTE
      END
    ELSE
      CASE
        WHEN start_offset > INTERVAL 0 SECOND THEN
          tig.end_ts + start_offset
        ELSE
          tig.end_ts
      END
  END AS end_ts,
  ROUND(avg_speed, 2) AS avg_speed
FROM
  times_by_id_and_groups tig
  JOIN irrgs_streets irs ON tig.id = irs.id;
