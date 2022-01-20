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
),
jams_streets AS (
  SELECT
    j.uuid,
    s.ntram
  FROM
    longest_unique_jams j
    JOIN `DATASET_NAME.PREFIX_streets` s ON ST_INTERSECTS (j.the_geom, s.the_geom)
),
jams_lifetimes AS (
  SELECT
    uuid,
    date(timestamp_trunc(georss_date, DAY)) AS georss_date,
    MIN(georss_date) AS start_ts,
    MAX(georss_date) AS end_ts
  FROM
    `DATASET_NAME.PREFIX_waze_data_jams`
  WHERE
    DATE(georss_date) = (current_date - 1)
  GROUP BY
    uuid,
    date(timestamp_trunc(georss_date, DAY))
)

SELECT
  ist.ntram,
  jl.start_ts,
  jl.end_ts
FROM
  jams_lifetimes jl
  JOIN jams_streets ist ON jl.uuid = ist.uuid
