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
),
irrgs_streets AS (
  SELECT
    i.id,
    s.ntram
  FROM
    longest_unique_irrgs i
    JOIN `DATASET_NAME.PREFIX_streets` s ON ST_INTERSECTS (i.the_geom, s.the_geom)
),
irrgs_lifetimes AS (
  SELECT
    id,
    date(timestamp_trunc(georss_date, DAY)) AS georss_date,
    MIN(detectiondate) AS start_ts,
    MAX(georss_date) AS end_ts
  FROM
    `DATASET_NAME.PREFIX_waze_data_irrgs`
  WHERE
    DATE(georss_date) = (current_date - 1)
  GROUP BY
    id,
    date(timestamp_trunc(georss_date, DAY))
)

SELECT
  ist.ntram,
  il.start_ts,
  il.end_ts
FROM
  irrgs_lifetimes il
  JOIN irrgs_streets ist ON il.id = ist.id
