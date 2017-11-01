/*
* Function to build historic accidents table (days).
*
* FUNCTION PARAMS:
*     @param     xxxxxxxxxxxxxxx
*
*/

--------------------------------------------------------------------------------
-- HOW TO USE:
--   SELECT build_historic_accid(
--        'carto_user', '2017-10-29T00:00:00Z'::timestamp
--       );
--------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS _create_historic_accid_table(text, text, boolean);
CREATE OR REPLACE FUNCTION _create_historic_accid_table(
  carto_user text,
  hist_days_tbl text,
  clean_table boolean DEFAULT FALSE
  )
  RETURNS void AS
  $$
DECLARE
  _tbl_days integer;
BEGIN

  IF clean_table then
    EXECUTE format('DROP TABLE IF EXISTS %I;', hist_days_tbl);
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
        street text,
        gdate_days timestamp without time zone,
        total_jams integer
      );
      SELECT CDB_Cartodbfytable(%2$L, %1$L);
      ', hist_days_tbl, carto_user);
  END IF;

END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS build_historic_accid(text, timestamp, text, text);
CREATE OR REPLACE FUNCTION build_historic_accid(
  carto_user text,
  tm_range timestamp,
  waze_alerts_tbl text DEFAULT 'waze_data_alerts',
  hist_days_tbl text DEFAULT 'waze_historic_accids_days'
  )
  RETURNS void AS
  $$
DECLARE
  _dt_hour text := 'hour';
  _dt_day text := 'day';
BEGIN

  EXECUTE _create_historic_accid_table(
    carto_user, hist_days_tbl
  );

  EXECUTE format('
    DELETE FROM %1$I WHERE gdate_days > %5$L::timestamp;
    INSERT INTO %1$I (
      the_geom,
      street,
      gdate_days,
      total_jams
    )
    SELECT
      ST_Union(distinct the_geom) as the_geom,
      street, date_trunc(%3$L, date) as gdate_days,
      count(*) as total_accid
    FROM %2$I
    WHERE date > %5$L::timestamp
    AND street != %4$L
    GROUP BY gdate_days, street
    ORDER BY gdate_days desc
    ', hist_days_tbl, waze_alerts_tbl, _dt_day,
    'NULL', tm_range);

END;
$$ LANGUAGE plpgsql;
