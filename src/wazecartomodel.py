"""
Carto Waze Lambda Connector

Developed by Geographica, 2017-2018.
"""

from src.cartomodel import CartoModel


class WazeCartoModel(CartoModel):

    def __init__(self, carto_api_key, carto_user, city_prefix):
        super(WazeCartoModel, self).__init__(carto_api_key, carto_user)

        self.__city_prefix = city_prefix
        self.__logger = self.load_logger(True)

    def store_alerts(self, alerts_data):

        if alerts_data:
            self.__logger.info('Storing Alerts data...')

            alerts_vl = [
                """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
                '{country}',E'{city}',E'{reportdescription}',{confidence},
                {reportrating},{reliability},'{date}'::timestamp,E'{street}',
                {roadtype},{magvar},{nthumbsup},E'{type}',E'{subtype}','{uuid}',
                '{jam_uuid}','{georss_date}'::timestamp)
                """.format(**alert) for alert in alerts_data
            ]

            sql = """
                INSERT INTO {0}_waze_data_alerts
                (the_geom, country, city, reportdescription, confidence,
                 reportrating, reliability, date, street, roadtype, magvar,
                 nthumbsup, type, subtype, uuid, jam_uuid, georss_date)
                VALUES {1}
                """.format(self.__city_prefix,
                           ','.join(alerts_vl).replace('None','NULL'))

            self.query(sql, write_qry=True)

    def store_jams(self, jams_data):

        if jams_data:
            self.__logger.info('Storing Jams data...')

            jams_vl = [
                """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
                '{country}',E'{city}',{speed},{length},{level},{delay},
                '{date}'::timestamp,E'{startnode}',E'{endnode}',
                E'{street}',{roadtype},'{type}','{turntype}','{uuid}',
                '{blockingalert_uuid}','{georss_date}'::timestamp)
                """.format(**jam) for jam in jams_data
            ]

            sql = """
                INSERT INTO {0}_waze_data_jams
                (the_geom, country, city, speed, length, level, delay, date,
                 startnode, endnode, street, roadtype, type, turntype, uuid,
                 blockingalert_uuid, georss_date)
                VALUES {1}
                """.format(self.__city_prefix,
                           ','.join(jams_vl).replace('None','NULL'))

            self.query(sql, write_qry=True)

    def store_irrgs(self, irrgs_data):

        if irrgs_data:
            self.__logger.info('Storing Irregularities data...')

            irrgs_vl = [
                """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
                '{country}',E'{city}',{speed},{regularspeed},{length},{jamlevel},
                {severity},{highway},{trend},{seconds},{delayseconds},
                '{detectiondate}'::timestamp,'{updatedate}'::timestamp,
                E'{startnode}',E'{endnode}',E'{street}',{ncomments},{nimages},
                {nthumbsup},{id},'{type}',{alertscount},
                ARRAY[{alerts_uuid}]::text[],'{georss_date}'::timestamp)
                """.format(**irrg) for irrg in irrgs_data
            ]

            sql = """
                INSERT INTO {0}_waze_data_irrgs
                (the_geom, country, city, speed, regularspeed, length, jamlevel,
                severity, highway, trend, seconds, delayseconds, detectiondate,
                updatedate, startnode, endnode, street, ncomments, nimages,
                nthumbsup, id, type, alertscount, alerts_uuid, georss_date)
                VALUES {1}
                """.format(self.__city_prefix,
                           ','.join(irrgs_vl).replace('None','NULL'))

            self.query(sql, write_qry=True)

    def refresh_mviews(self):

        sql = """
            REFRESH MATERIALIZED VIEW {0}_waze_data_alerts;
            REFRESH MATERIALIZED VIEW {0}_waze_data_jams;
            REFRESH MATERIALIZED VIEW {0}_waze_data_irrgs;
            """.format(self.__city_prefix)

        self.query(sql, write_qry=True)
