"""
Carto Waze Lambda Connector

Developed by Geographica, 2017-2018.
"""

from src.models.cartomodel import CartoModel
from datetime import datetime, timedelta, timezone


class WazeCartoModel(CartoModel):
    def __init__(self, carto_api_key, carto_user, city_prefix, prune_max_hours):
        super(WazeCartoModel, self).__init__(carto_api_key, carto_user)

        self.__city_prefix = city_prefix
        self.__prune_max_hours = prune_max_hours

    def store_alerts(self, alerts_data):
        alerts_table_suffix = 'waze_data_alerts'

        if alerts_data:
            self._logger.info('Storing Alerts data...')

            alerts_vl = [
                """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
                '{country}',E'{city}',E'{reportdescription}',{confidence},
                {reportrating},{reliability},'{date}'::timestamp,E'{street}',
                {roadtype},{magvar},{nthumbsup},E'{type}',E'{subtype}','{uuid}',
                '{jam_uuid}','{georss_date}'::timestamp)
                """.format(
                    **alert
                )
                for alert in alerts_data
            ]

            sql = """
                INSERT INTO {0}_{1}
                (the_geom, country, city, reportdescription, confidence,
                 reportrating, reliability, date, street, roadtype, magvar,
                 nthumbsup, type, subtype, uuid, jam_uuid, georss_date)
                VALUES {2}
            """.format(
                self.__city_prefix,
                alerts_table_suffix,
                ','.join(alerts_vl).replace('None', 'NULL'),
            )

            self.query(sql, write_qry=True)

            self._logger.info('Alerts data stored in CARTO account!')

        if self.__prune_max_hours:
            self._logger.info(
                f'Deleting historic alerts data older than {self.__prune_max_hours} hours from CARTO...'
            )

            sql = self.__get_prune_sql_str(alerts_table_suffix)

            self.query(sql, write_qry=True)

            self._logger.info('Historic alerts records removed from CARTO!')

    def store_jams(self, jams_data):
        jams_table_suffix = 'waze_data_jams'

        if jams_data:
            self._logger.info('Storing Jams data...')

            jams_vl = [
                """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
                '{country}',E'{city}',{speed},{length},{level},{delay},
                '{date}'::timestamp,E'{startnode}',E'{endnode}',
                E'{street}',{roadtype},'{type}','{turntype}','{uuid}',
                '{blockingalert_uuid}','{georss_date}'::timestamp)
                """.format(
                    **jam
                )
                for jam in jams_data
            ]

            sql = """
                INSERT INTO {0}_{1}
                (the_geom, country, city, speed, length, level, delay, date,
                 startnode, endnode, street, roadtype, type, turntype, uuid,
                 blockingalert_uuid, georss_date)
                VALUES {2}
            """.format(
                self.__city_prefix,
                jams_table_suffix,
                ','.join(jams_vl).replace('None', 'NULL'),
            )

            self.query(sql, write_qry=True)

        if self.__prune_max_hours:
            self._logger.info(
                f'Deleting historic jams data older than {self.__prune_max_hours} hours from CARTO...'
            )

            sql = self.__get_prune_sql_str(jams_table_suffix)

            self.query(sql, write_qry=True)

            self._logger.info('Historic jams records removed from CARTO!')

    def store_irrgs(self, irrgs_data):
        irrgs_table_suffix = 'waze_data_irrgs'

        if irrgs_data:
            self._logger.info('Storing Irregularities data...')

            irrgs_vl = [
                """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
                '{country}',E'{city}',{speed},{regularspeed},{length},{jamlevel},
                {severity},{highway},{trend},{seconds},{delayseconds},
                '{detectiondate}'::timestamp,'{updatedate}'::timestamp,
                E'{startnode}',E'{endnode}',E'{street}',{ncomments},{nimages},
                {nthumbsup},{id},'{type}',{alertscount},
                ARRAY[{alerts_uuid}]::text[],'{georss_date}'::timestamp)
                """.format(
                    **irrg
                )
                for irrg in irrgs_data
            ]

            sql = """
                INSERT INTO {0}_{1}
                (the_geom, country, city, speed, regularspeed, length, jamlevel,
                severity, highway, trend, seconds, delayseconds, detectiondate,
                updatedate, startnode, endnode, street, ncomments, nimages,
                nthumbsup, id, type, alertscount, alerts_uuid, georss_date)
                VALUES {2}
                """.format(
                self.__city_prefix,
                irrgs_table_suffix,
                ','.join(irrgs_vl).replace('None', 'NULL'),
            )

            self.query(sql, write_qry=True)

        if self.__prune_max_hours:
            self._logger.info(
                f'Deleting historic irregularities data older than {self.__prune_max_hours} hours from CARTO...'
            )

            sql = self.__get_prune_sql_str(irrgs_table_suffix)

            self.query(sql, write_qry=True)

            self._logger.info('Historic irregularities records removed from CARTO!')

    def refresh_mviews(self):

        sql = """
            REFRESH MATERIALIZED VIEW {0}_waze_data_alerts_mv;
            REFRESH MATERIALIZED VIEW {0}_waze_data_jams_mv;
            REFRESH MATERIALIZED VIEW {0}_waze_data_irrgs_mv;
            """.format(
            self.__city_prefix
        )

        self.query(sql, write_qry=True)

    def __get_prune_sql_str(self, table_suffix):
        prune_dt_str = (
            datetime.now(timezone.utc) - timedelta(hours=float(self.__prune_max_hours))
        ).isoformat()

        return f"""
            DELETE FROM {self.__city_prefix}_{table_suffix}
            WHERE georss_date <= '{prune_dt_str}'::timestamp
        """

    def store_aggregated_jams_by_hour(self, data):
        insert_values = []

        if not data:
            return

        self._logger.info('Creating INSERT statements for jams_agg_hour...')

        for row in data:
            a_type_part = (
                f"ARRAY{list(row.alert_types)}, " if row.alert_types else 'NULL, '
            )
            a_subtype_part = (
                f"ARRAY{list(row.alert_subtypes)}, " if row.alert_subtypes else 'NULL, '
            )

            value_stmt = (
                '('
                + f"{row.ntram}, '{row.georss_date}'::timestamp without time zone, "
                + f"{row.avg_level}, {row.avg_speed}, {row.avg_length}, "
                + f"{row.duration_seconds}, "
                + a_type_part
                + a_subtype_part
                + f"{row.road_type}"
                + ')'
            )

            insert_values.append(value_stmt)

        chunk_size = 1000
        chunks = [insert_values[i:i+chunk_size] for i in range(0, len(insert_values), chunk_size)]
        n_chunks = len(chunks)

        for chunk_n, chunk in enumerate(chunks, start=1):
            insert_query = f"""
                INSERT INTO {self.__city_prefix}_waze_data_jams_agg_hour
                (ntram, georss_date, avg_level, avg_speed, avg_length,
                duration_seconds, alert_types, alert_subtypes, road_type)
                VALUES {','.join(chunk).replace('None', 'NULL')}
            """

            self._logger.info(
                f'INSERT statement generated for chunk {chunk_n}/{n_chunks}'
            )

            self._logger.info(f'Executing INSERT statement for chunk {chunk_n}/{n_chunks}...')

            self.query(insert_query, write_qry=True)

            self._logger.info(f'INSERT statement for chunk {chunk_n}/{n_chunks} executed!')

        self._logger.info('INSERT statements executed successfully for jams_agg_hour!')

    def store_aggregated_jams_durations(self, data):
        insert_values = []

        if not data:
            return

        self._logger.info('Creating INSERT statement for jams_agg_times...')

        for row in data:
            value_stmt = (
                '('
                + f"'{row.ntram}', "
                + f"'{row.start_ts}'::timestamp without time zone, "
                + f"'{row.end_ts}'::timestamp without time zone"
                + ')'
            )

            insert_values.append(value_stmt)

        chunk_size = 1000
        chunks = [insert_values[i:i+chunk_size] for i in range(0, len(insert_values), chunk_size)]
        n_chunks = len(chunks)

        for chunk_n, chunk in enumerate(chunks, start=1):
            insert_query = f"""
                INSERT INTO {self.__city_prefix}_waze_data_jams_agg_times
                (ntram, start_ts, end_ts)
                VALUES {','.join(chunk).replace('None', 'NULL')}
            """

            self._logger.info(
                f'INSERT statement generated for chunk {chunk_n}/{n_chunks}'
            )

            self._logger.info(f'Executing INSERT statement for chunk {chunk_n}/{n_chunks}...')

            self.query(insert_query, write_qry=True)

            self._logger.info(f'INSERT statement for chunk {chunk_n}/{n_chunks} executed!')

        self._logger.info('INSERT statement executed successfully for jams_agg_times!')

    def store_aggregated_jams_levels_times(self, data):
        insert_values = []

        if not data:
            return

        self._logger.info('Creating INSERT statement for jams_agg_levels_times...')

        for row in data:
            value_stmt = (
                '('
                + f"'{row.id}', "
                + f"{row.ntram}, "
                + f"{row.level}, "
                + f"'{row.start_ts}'::timestamp without time zone, "
                + f"'{row.end_ts}'::timestamp without time zone, "
                + f"{row.avg_speed}"
                + ')'
            )

            insert_values.append(value_stmt)

        chunk_size = 1000
        chunks = [insert_values[i:i+chunk_size] for i in range(0, len(insert_values), chunk_size)]
        n_chunks = len(chunks)

        for chunk_n, chunk in enumerate(chunks, start=1):
            insert_query = f"""
                INSERT INTO {self.__city_prefix}_waze_data_jams_agg_levels_times
                (id, ntram, level, start_ts, end_ts, avg_speed)
                VALUES {','.join(chunk).replace('None', 'NULL')}
            """

            self._logger.info(
                f'INSERT statement generated for chunk {chunk_n}/{n_chunks}'
            )

            self._logger.info(f'Executing INSERT statement for chunk {chunk_n}/{n_chunks}...')

            self.query(insert_query, write_qry=True)

            self._logger.info(f'INSERT statement for chunk {chunk_n}/{n_chunks} executed!')

        self._logger.info('INSERT statement executed successfully for jams_agg_levels_times!')
