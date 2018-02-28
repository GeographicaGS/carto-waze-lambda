/*
* Function to create data model for a city.
*
* FUNCTION PARAMS:
*     @param     city_prefix: table name prefix for this city.
*     @param     carto_user: user for this carto account (With Trial accounts
  *                you must not use this parameter).
*     @param     clean_tables: drop all tables for this city prefix.
*
*/

--------------------------------------------------------------------------------
-- HOW TO USE:
--   SELECT traffico_create_tables(
--        'mycity', clean_tables:=FALSE
--       );
--------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS traffico_create_tables(text, text, boolean);
CREATE OR REPLACE FUNCTION traffico_create_tables(
  city_prefix text,
  carto_user text DEFAULT NULL::text,
  clean_tables boolean DEFAULT FALSE
  )
  RETURNS void AS
  $$
DECLARE
  _alerts_tb text;
  _jams_tb text;
  _irregs_tb text;
  _carto_params_al text;
  _carto_params_jm text;
  _carto_params_ir text;
BEGIN

  _alerts_tb = format('%s_waze_data_alerts', city_prefix);
  _jams_tb = format('%s_waze_data_jams', city_prefix);
  _irregs_tb = format('%s_waze_data_irrgs', city_prefix);

  _carto_params_al = format('%L', _alerts_tb);
  _carto_params_jm = format('%L', _jams_tb);
  _carto_params_ir = format('%L', _irregs_tb);

  IF carto_user then
    _carto_params_al = format('%L, %s', city_prefix, _carto_params_al);
    _carto_params_jm = format('%L, %s', city_prefix, _carto_params_jm);
    _carto_params_ir = format('%L, %s', city_prefix, _carto_params_ir);
  END IF;

  IF clean_tables then
    EXECUTE format('
      DROP TABLE IF EXISTS %1$I CASCADE;
      DROP TABLE IF EXISTS %2$I CASCADE;
      DROP TABLE IF EXISTS %3$I CASCADE;
      ', _alerts_tb, _jams_tb, _irregs_tb
    );
  END IF;

  EXECUTE format('
    CREATE TABLE %1$I (
      country text,
      city text,
      reportdescription text,
      confidence integer,
      reportrating integer,
      reliability integer,
      date timestamp without time zone,
      street text,
      roadtype integer,
      magvar integer,
      nthumbsup integer,
      type text,
      subtype text,
      uuid text,
      jam_uuid text,
      georss_date timestamp without time zone
    );

    SELECT CDB_Cartodbfytable( %2$s );

    CREATE TABLE %3$I (
      country text,
      city text,
      speed double precision,
      length double precision,
      level integer,
      delay integer,
      date timestamp without time zone,
      startnode text,
      endnode text,
      street text,
      roadtype integer,
      type text,
      turntype text,
      uuid text,
      blockingalert_uuid text,
      georss_date timestamp without time zone
    );

    SELECT CDB_Cartodbfytable( %4$s );

    CREATE TABLE %5$I (
      country text,
      city text,
      speed double precision,
      regularspeed double precision,
      length double precision,
      jamlevel integer,
      severity integer,
      highway boolean,
      trend integer,
      seconds integer,
      delayseconds integer,
      detectiondate timestamp without time zone,
      updatedate timestamp without time zone,
      startnode text,
      endnode text,
      street text,
      ncomments integer,
      nimages integer,
      nthumbsup integer,
      id bigint,
      type text,
      alertscount integer,
      alerts_uuid text[],
      georss_date timestamp without time zone
    );

    SELECT CDB_Cartodbfytable( %6$s );
    ', _alerts_tb, _carto_params_al, _jams_tb,
      _carto_params_jm, _irregs_tb, _carto_params_ir
    );

  EXECUTE format('
    -- alerts
    CREATE INDEX %1$s_wd_alerts_georssdate_idx
      ON %2$I (georss_date);

    CREATE INDEX %1$s_wd_alerts_date_idx
      ON %2$I (date);

    -- jams
    CREATE INDEX %1$s_wd_jams_georssdate_idx
      ON %3$I (georss_date);

    CREATE INDEX %1$s_wd_jams_date_idx
      ON %3$I (date);

    -- irregularities
    CREATE INDEX %1$s_wd_irrgs_georssdate_idx
      ON %4$I (georss_date);

    CREATE INDEX %1$s_wd_irrgs_update_idx
      ON %4$I (updatedate);

    CREATE INDEX %1$s_wd_irrgs_detdate_idx
      ON %4$I (detectiondate);
    ', city_prefix, _alerts_tb, _jams_tb, _irregs_tb
    );

END;
$$ LANGUAGE plpgsql;
