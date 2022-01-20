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



DROP FUNCTION IF EXISTS traffico_create_mviews(text);
CREATE OR REPLACE FUNCTION traffico_create_mviews(
  city_prefix text
  )
  RETURNS void AS
  $$
DECLARE
  _table text;
  _tables text[];
  _arr_low text[];
  _arr_medium text[];
  _arr_high text[];
  _alerts_type text;
  _res_q text;
BEGIN

  _tables = ARRAY[
      format('%s_waze_data_alerts', city_prefix),
      format('%s_waze_data_jams', city_prefix),
      format('%s_waze_data_irrgs', city_prefix)
    ]::text[];

  _arr_low = ARRAY[
    'WEATHER_HEAT_WAVE',
    'HAZARD_ON_SHOULDER_MISSING_SIGN',
    'HAZARD_ON_SHOULDER_ANIMALS',
    'HAZARD_ON_SHOULDER_CAR_STOPPED',
    'HAZARD_ON_ROAD_POT_HOLE',
    'HAZARD_ON_ROAD_OBJECT',
    'HAZARD_ON_SHOULDER',
    'HAZARD_ON_ROAD',
    'ROAD_CLOSED_EVENT',
    'JAM_LIGHT_TRAFFIC',
    'ACCIDENT_MINOR'
  ];
  _arr_medium = ARRAY[
    'HAZARD_ON_ROAD_CONSTRUCTION',
    'HAZARD_ON_ROAD_ICE',
    'HAZARD_ON_ROAD_OIL',
    'HAZARD_ON_ROAD_LANE_CLOSED',
    'HAZARD_WEATHER_FREEZING_RAIN',
    'HAZARD_WEATHER_HAIL',
    'HAZARD_WEATHER_FOG',
    'ROAD_CLOSED_CONSTRUCTION',
    'JAM_MODERATE_TRAFFIC'
  ];
  _arr_high = ARRAY[
    'HAZARD_ON_ROAD_CAR_STOPPED',
    'HAZARD_WEATHER_HURRICANE',
    'HAZARD_WEATHER_TORNADO',
    'HAZARD_WEATHER_MONSOON',
    'HAZARD_WEATHER_FLOOD',
    'HAZARD_WEATHER_HEAVY_SNOW',
    'HAZARD_WEATHER_HEAVY_RAIN',
    'HAZARD_ON_ROAD_ROAD_KILL',
    'ROAD_CLOSED_HAZARD',
    'JAM_HEAVY_TRAFFIC',
    'JAM_STAND_STILL_TRAFFIC',
    'ACCIDENT_MAJOR'
  ];

  FOREACH _table IN ARRAY _tables
    LOOP
      IF _table ~ '_alerts$' then
        _alerts_type = format('
          ,CASE
            WHEN subtype = ANY(%1$L) THEN type || %2$L
            WHEN subtype = ANY(%3$L) THEN type || %4$L
            WHEN subtype = ANY(%5$L) THEN type || %6$L
            ELSE type || %2$L
          END as type_level
        ', _arr_low, '-low', _arr_medium, '-medium',
          _arr_high, '-high'
        );
      ELSE
        _alerts_type = '';
      END IF;

      EXECUTE format('
        DROP MATERIALIZED VIEW IF EXISTS %1$s_mv;
        CREATE MATERIALIZED VIEW %1$s_mv AS (
          SELECT * %2$s FROM %1$s WHERE georss_date IS NOT NULL
          AND georss_date=(SELECT MAX(georss_date) FROM %1$s)
        );
        CREATE INDEX %1$s_geom_idx
          ON %1$s_mv USING gist (the_geom);
        CREATE INDEX %1$s_geomwm_idx
          ON %1$s_mv USING gist (the_geom_webmercator);
        CREATE INDEX %1$s_cdbid_idx
          ON %1$s_mv (cartodb_id);
        CREATE INDEX %1$s_georssdate_idx
          ON %1$s_mv (georss_date);

        GRANT SELECT ON %1$s_mv TO publicuser;
        ', _table, _alerts_type);
    END LOOP;

END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS traffico_create_historic_agg_tables(text, boolean);
CREATE OR REPLACE FUNCTION traffico_create_historic_agg_tables(
  city_prefix text,
  clean_tables boolean DEFAULT FALSE
  )
  RETURNS void AS
  $$
DECLARE
  _jams_agg_tb text;
  _jams_agg_times_tb text;
  _jams_agg_levels_times_tb text;
BEGIN
  _jams_agg_tb = format('%s_waze_data_jams_agg_hour', city_prefix);
  _jams_agg_times_tb = format('%s_waze_data_jams_agg_times', city_prefix);
  _jams_agg_levels_times_tb = format('%s_waze_data_jams_agg_levels_times', city_prefix);

  IF clean_tables THEN
    EXECUTE format('
      DROP TABLE IF EXISTS %1$I CASCADE;
      DROP TABLE IF EXISTS %2$I CASCADE;
      DROP TABLE IF EXISTS %3$I CASCADE;
      ', _jams_agg_tb, _jams_agg_times_tb, _jams_agg_levels_times_tb
    );
  END IF;

  EXECUTE format('
    CREATE TABLE %1$I (
      georss_date timestamp without time zone,
      ntram integer,
      avg_level integer,
      avg_speed double precision,
      avg_length double precision,
      duration_seconds integer,
      alert_types text[],
      alert_subtypes text[],
      road_type integer
    ) PARTITION BY RANGE(georss_date);

    CREATE TABLE %2$I (
      ntram integer,
      start_ts timestamp without time zone,
      end_ts timestamp without time zone
    ) PARTITION BY RANGE(start_ts);

    CREATE TABLE %3$I (
      id text,
      ntram integer,
      level integer,
      start_ts timestamp without time zone,
      end_ts timestamp without time zone,
      avg_speed double precision
    ) PARTITION BY RANGE(start_ts);

    ', _jams_agg_tb, _jams_agg_times_tb, _jams_agg_levels_times_tb
  );

  EXECUTE format('
    -- jams agg

    CREATE INDEX ON %1$I (georss_date);

    CREATE INDEX ON %1$I ((georss_date::date));

    CREATE INDEX ON %1$I (ntram);

    -- jams times agg

    CREATE INDEX ON %2$I (start_ts);

    CREATE INDEX ON %2$I (end_ts);

    CREATE INDEX ON %2$I (ntram);

    -- jams levels times agg

    CREATE INDEX ON %3$I (start_ts);

    CREATE INDEX ON %3$I (end_ts);

    CREATE INDEX ON %3$I (ntram);
    ', _jams_agg_tb, _jams_agg_times_tb, _jams_agg_levels_times_tb
  );

  EXECUTE format('
    -- jams agg
    GRANT SELECT ON %1$I TO publicuser;

    -- jams times agg
    GRANT SELECT ON %2$I TO publicuser;

    -- jams times agg
    GRANT SELECT ON %3$I TO publicuser;
    ', _jams_agg_tb, _jams_agg_times_tb, _jams_agg_levels_times_tb
  );

  RAISE NOTICE 'PARTITIONS ON %1, %2 AND %3 MUST BE CREATED MANUALLY',
    _jams_agg_tb, _jams_agg_times_tb, _jams_agg_levels_times_tb;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS traffico_create_tables(text, text, boolean, boolean, boolean);
CREATE OR REPLACE FUNCTION traffico_create_tables(
  city_prefix text,
  carto_user text DEFAULT NULL::text,
  clean_tables boolean DEFAULT FALSE,
  create_mviews boolean DEFAULT TRUE,
  create_historic_agg_tables boolean DEFAULT FALSE
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

  IF carto_user IS NOT NULL then
    _carto_params_al = format('%L, %s', carto_user, _carto_params_al);
    _carto_params_jm = format('%L, %s', carto_user, _carto_params_jm);
    _carto_params_ir = format('%L, %s', carto_user, _carto_params_ir);
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

  IF create_mviews then
    EXECUTE traffico_create_mviews(city_prefix);
  END IF;

  IF create_historic_agg_tables then
    EXECUTE traffico_create_historic_agg_tables(city_prefix, clean_tables);
  END IF;

END;
$$ LANGUAGE plpgsql;
