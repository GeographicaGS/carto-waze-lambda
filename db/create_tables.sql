
-- Example:
-- {'city': 'Madrid',
--   'confidence': 0,
--   'country': 'SP',
--   'location': {'x': -3.682701, 'y': 40.421694},
--   'magvar': 239,
--   'nThumbsUp': 0,
--   'pubMillis': 1508409488722,
--   'reliability': 6,
--   'reportRating': 3,
--   'roadType': 7,
--   'street': 'Calle de Alcalá',
--   'subtype': 'HAZARD_ON_ROAD_CONSTRUCTION',
--   'type': 'WEATHERHAZARD',
--   'uuid': 'e564dca1-0c34-380b-b357-6b825f30ea58'}
CREATE TABLE waze_data_alerts (
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

CREATE INDEX waze_data_alerts_georssdate_idx
  ON waze_data_alerts(georss_date);

SELECT CDB_Cartodbfytable('user', 'waze_data_alerts');

-- Example:
-- { 'city': 'Madrid',
--   'country': 'SP',
--   'delay': 90,
--   'endNode': 'Calle de Deyanira',
--   'length': 651,
--   'level': 3,
--   'line': [{'x': -3.586018, 'y': 40.448805},
--    {'x': -3.586051, 'y': 40.447798},
--    {'x': -3.586084, 'y': 40.446794},
--    {'x': -3.586094, 'y': 40.446465},
--    {'x': -3.586113, 'y': 40.44592},
--    {'x': -3.586116, 'y': 40.445831},
--    {'x': -3.586142, 'y': 40.445076},
--    {'x': -3.58615, 'y': 40.4449},
--    {'x': -3.586169, 'y': 40.444417},
--    {'x': -3.586173, 'y': 40.444315},
--    {'x': -3.586259, 'y': 40.442953}],
--   'pubMillis': 1508411029051,
--   'roadType': 1,
--   'segments': [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}],
--   'speed': 3.513888888888889,
--   'street': 'Calle de Arrastaría',
--   'turnType': 'NONE',
--   'type': 'NONE',
--   'uuid': 2061827729}
CREATE TABLE waze_data_jams (
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

CREATE INDEX waze_data_jams_georssdate_idx
  ON waze_data_jams(georss_date);

CREATE INDEX waze_data_jams_date_idx
  ON waze_data_jams(date);

SELECT CDB_Cartodbfytable('user', 'waze_data_jams');

-- Example:
-- {'alerts': [...],
--   'alertsCount': 1,
--   'city': 'Madrid',
--   'country': 'SP',
--   'delaySeconds': 376,
--   'detectionDate': 'Thu Oct 19 09:48:32 +0000 2017',
--   'detectionDateMillis': 1508406512566,
--   'driversCount': 29,
--   'endNode': 'Calle del Príncipe de Vergara',
--   'highway': False,
--   'id': 132746691,
--   'jamLevel': 4,
--   'length': 613,
--   'line': [{'x': -3.686734, 'y': 40.43298},
--    {'x': -3.686426, 'y': 40.432911},
--    {'x': -3.685597, 'y': 40.432845},
--    {'x': -3.684674, 'y': 40.432822},
--    {'x': -3.683582, 'y': 40.432771},
--    {'x': -3.682159, 'y': 40.432685},
--    {'x': -3.680853, 'y': 40.432632},
--    {'x': -3.679533, 'y': 40.432557}],
--   'nComments': 0,
--   'nImages': 0,
--   'nThumbsUp': 0,
--   'regularSpeed': 11.69,
--   'seconds': 468,
--   'severity': 1,
--   'speed': 4.71,
--   'street': 'Calle de Juan Bravo',
--   'trend': -1,
--   'type': 'Small',
--   'updateDate': 'Thu Oct 19 11:15:59 +0000 2017',
--   'updateDateMillis': 1508411759945}
CREATE TABLE waze_data_irrgs (
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

CREATE INDEX waze_data_irrgs_georssdate_idx
  ON waze_data_irrgs(georss_date);

CREATE INDEX waze_data_irrgs_update_idx
  ON waze_data_irrgs(updatedate);

CREATE INDEX waze_data_irrgs_detdate_idx
  ON waze_data_irrgs(detectiondate);

SELECT CDB_Cartodbfytable('user', 'waze_data_irrgs');
