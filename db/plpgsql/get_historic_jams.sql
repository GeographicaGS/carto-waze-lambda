/*
* Function to get historic jams.
*
* FUNCTION PARAMS:
*     @param     xxxxxxxxxxxxxxx
*
*/

--------------------------------------------------------------------------------
-- HOW TO USE:
-- SELECT hours_json, hours_agg, street, level, total_days, geojson
-- FROM get_historic_jams(
--   10, '2017-10-19T00:00Z'::timestamp,
--   '2017-10-31T00:00Z'::timestamp, ARRAY[1,2,3]
-- ) as (
--   hours_json json, hours_agg integer[],
--   street text, level integer, total_days integer,
--   geojson jsonb
-- ) order by level desc, total_days desc;
--------------------------------------------------------------------------------


DROP AGGREGATE array_concat_agg(anyarray);
CREATE AGGREGATE array_concat_agg(anyarray) (
  SFUNC = array_cat,
  STYPE = anyarray
);

DROP FUNCTION IF EXISTS get_historic_jams(integer, timestamp, timestamp,
  integer[]);
CREATE OR REPLACE FUNCTION get_historic_jams(
  n_items integer,
  tm_start timestamp,
  tm_end timestamp,
  levels integer[] DEFAULT ARRAY[1, 2, 3]
  )
  RETURNS SETOF record AS
  $$
DECLARE
  _rec record;
  _hjams_q text;
  _hours_tot integer[];
  _lv integer;
BEGIN

  FOREACH _lv IN ARRAY levels
    LOOP
      _hjams_q = format('
        WITH _q As (
          SELECT
            street, count(*) as total_days, level,
            array_concat_agg(hours) as agg_hours,
            json_agg(
              json_build_object(gdate_days, hours)
            ) as json_hours, the_geom
          FROM waze_historic_jams_days
          WHERE gdate_days > %1$L::timestamp
            AND gdate_days < %2$L::timestamp
            AND level = %4$s
          GROUP BY level, street, the_geom
        )
        SELECT
          street, total_days, level, agg_hours, json_hours,
          array_length(agg_hours, 1) as agg_hr_ln,
          ST_AsGeoJSON(the_geom)::jsonb as geojson
        FROM _q
        ORDER BY total_days desc, agg_hr_ln desc, street
        LIMIT %3$s
        ', tm_start, tm_end, n_items, _lv
      );

      FOR _rec IN EXECUTE _hjams_q
        LOOP
          _hours_tot = array_agg(DISTINCT arr)
              FROM unnest(_rec.agg_hours) as arr;

          RETURN NEXT ROW(
            _rec.json_hours,
            _hours_tot,
            _rec.street,
            _rec.level,
            _rec.total_days::integer,
            _rec.geojson
          );

        END LOOP;

    END LOOP;

END;
$$ LANGUAGE plpgsql;
