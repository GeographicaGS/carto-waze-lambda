WITH id_partitioned_jams AS (
  SELECT
    uuid,
    the_geom,
    ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY length DESC) AS r_n
  FROM
    `DATASET_NAME.PREFIX_waze_data_jams`
  WHERE
    DATE(georss_date) = (current_date - 1)
),
longest_unique_jams AS (
  SELECT
    uuid,
    the_geom
  FROM
    id_partitioned_jams
  WHERE
    r_n = 1
), jams_streets AS (
  SELECT
    j.uuid,
    s.ntram
  FROM
    longest_unique_jams j
    JOIN `DATASET_NAME.PREFIX_streets` s ON ST_INTERSECTS (j.the_geom, s.the_geom)
), data_by_id_and_groups AS (
  SELECT
    uuid,
    level,
    georss_date,
    LAG(georss_date) OVER (PARTITION BY uuid, level ORDER BY georss_date) AS prev_ts_group,
    AVG(speed) OVER (PARTITION BY uuid, level) AS avg_speed,
    MIN(georss_date) OVER (PARTITION BY uuid) AS first_georss_ts,
    MIN(date) OVER (PARTITION BY uuid) AS first_ts,
    MAX(georss_date) OVER (PARTITION BY uuid) AS last_ts
  FROM
    `DATASET_NAME.PREFIX_waze_data_jams`
  WHERE
    DATE(georss_date) = (CURRENT_DATE - 1)
  ORDER BY
    georss_date ASC
), times_by_id_and_groups AS (
SELECT
  uuid,
  level,
  georss_date,
  avg_speed,
  LEAD(georss_date) OVER (PARTITION BY uuid ORDER BY georss_date) AS end_ts,
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
  tig.uuid AS id,
  js.ntram,
  tig.level,
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
  JOIN jams_streets js ON tig.uuid = js.uuid;
