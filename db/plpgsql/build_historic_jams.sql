/*
* Function to build historic jams tables (days, hours).
*
* FUNCTION PARAMS:
*     @param     xxxxxxxxxxxxxxx
*
*/

--------------------------------------------------------------------------------
-- HOW TO USE:
--   SELECT build_historic_jams(
--        'carto_user', '2017-10-29T00:00:00Z'::timestamp
--       );
--------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS _create_historic_jam_tables(text, text, text, boolean);
CREATE OR REPLACE FUNCTION _create_historic_jam_tables(
  carto_user text,
  hist_hours_tbl text,
  hist_days_tbl text,
  clean_tables boolean DEFAULT FALSE
  )
  RETURNS void AS
  $$
DECLARE
  _tbl_hours integer;
  _tbl_days integer;
BEGIN

  IF clean_tables then
    EXECUTE format('DROP TABLE IF EXISTS %I;', hist_hours_tbl);
    EXECUTE format('DROP TABLE IF EXISTS %I;', hist_days_tbl);
  END IF;

  EXECUTE format('
    SELECT 1 FROM CDB_UserTables(%1$L)
    WHERE cdb_usertables = %2$L;'
   , 'all', hist_hours_tbl)
   INTO _tbl_hours;

  IF _tbl_hours IS NULL then
    EXECUTE format('
      CREATE TABLE %1$I (
        the_geom geometry,
        level integer,
        street text,
        gdate_hours timestamp without time zone,
        total_jams integer,
        n_uuid integer,
        n_geom integer
      );
      SELECT CDB_Cartodbfytable(%2$L, %1$L);
      ', hist_hours_tbl, carto_user);
  END IF;

  EXECUTE format('
    SELECT 1 FROM CDB_UserTables(%1$L)
    WHERE cdb_usertables = %2$L;'
   , 'all', hist_days_tbl)
   INTO _tbl_days;

  IF _tbl_days IS NULL then
    EXECUTE format('
      CREATE TABLE %1$I (
        the_geom geometry,
        level integer,
        street text,
        gdate_days timestamp without time zone,
        total_jams integer,
        hours integer[]
      );
      SELECT CDB_Cartodbfytable(%2$L, %1$L);
      ', hist_days_tbl, carto_user);
  END IF;

END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS build_historic_jams(text, timestamp, text, text, text);
CREATE OR REPLACE FUNCTION build_historic_jams(
  carto_user text,
  tm_range timestamp,
  waze_jams_tbl text DEFAULT 'waze_data_jams',
  hist_hours_tbl text DEFAULT 'waze_historic_jams_hours',
  hist_days_tbl text DEFAULT 'waze_historic_jams_days'
  )
  RETURNS void AS
  $$
DECLARE
  _dt_hour text := 'hour';
  _dt_day text := 'day';
BEGIN

  EXECUTE _create_historic_jam_tables(
    carto_user, hist_hours_tbl, hist_days_tbl
  );

  EXECUTE format('
    DELETE FROM %1$I WHERE gdate_hours > %4$L::timestamp;
    INSERT INTO %1$I (
      n_uuid,
      n_geom,
      the_geom,
      level,
      street,
      gdate_hours,
      total_jams
    )
    SELECT
      array_length(array_agg(distinct uuid), 1) as n_uuid,
      array_length(array_agg(distinct the_geom), 1) as n_geom,
      ST_Union(distinct the_geom) as the_geom,
      CASE
        WHEN level IN (4,5) THEN 3
        WHEN level IN (2,3) THEN 2
        ELSE 1
      END as level,
      street,
      date_trunc(%3$L, date) as gdate_hours,
      count(*) as total_jams
    FROM %2$I
    WHERE date > %4$L::timestamp
    AND date IS NOT NULL
    AND street != %5$L
    GROUP BY gdate_hours, street, level
    ORDER BY total_jams desc
    ', hist_hours_tbl, waze_jams_tbl, _dt_hour,
      tm_range, 'NULL');

  EXECUTE format('
    DELETE FROM %1$I WHERE gdate_days > %5$L::timestamp;
    INSERT INTO %1$I (
      the_geom,
      level,
      street,
      gdate_days,
      total_jams,
      hours
    )
    SELECT
      ST_Union(distinct the_geom) as the_geom,
      level,
      street,
      date_trunc(%3$L, gdate_hours) as gdate_days,
      count(*) as total_jams,
      array_agg(DISTINCT date_part(%4$L, gdate_hours)) as  hours
    FROM %2$I
    WHERE gdate_hours > %5$L::timestamp
    GROUP BY gdate_days, street, level
    ORDER BY total_jams desc
    ', hist_days_tbl, hist_hours_tbl, _dt_day, _dt_hour,
    tm_range);

END;
$$ LANGUAGE plpgsql;
