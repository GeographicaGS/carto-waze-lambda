import datetime

from google.cloud import bigquery
from src.models.base import Model


class WazeBigQueryModelException(Exception):
    pass


class WazeBigQueryModel(Model):
    def __init__(self, project, dataset, city_prefix, verbose=True):
        super().__init__(verbose)

        self.__client = bigquery.Client.from_service_account_json(
            'gcloud-credentials.json'
        )

        if not project or not dataset:
            raise WazeBigQueryModelException(
                'A valid project and dataset attributes must be provided in order to write in Google Big Query'
            )

        self.__table_id_prefix = f'{project}.{dataset}'
        self.__city_prefix = city_prefix

    def store_alerts(self, alerts_data):
        if alerts_data:
            self._logger.info('Storing Alerts data in Big Query...')

            table_id = self.__client.get_table(
                f'{self.__table_id_prefix}.{self.__city_prefix}_waze_data_alerts'
            )

            insert_errors = self.__client.insert_rows(
                table_id, alerts_data, row_ids=[None] * len(alerts_data)
            )

            if insert_errors:
                self._logger.error(
                    f'Could not insert all rows from data, {len(insert_errors)} rows ignored'
                )
            else:
                self._logger.info('Alerts data successfully stored in Big Query!')

    def store_jams(self, jams_data):
        if jams_data:
            self._logger.info('Storing Jams data in Big Query...')

            table_id = self.__client.get_table(
                f'{self.__table_id_prefix}.{self.__city_prefix}_waze_data_jams'
            )

            insert_errors = self.__client.insert_rows(
                table_id, jams_data, row_ids=[None] * len(jams_data)
            )

            if insert_errors:
                self._logger.error(
                    f'Could not insert all rows from data, {len(insert_errors)} rows ignored'
                )
            else:
                self._logger.info('Jams data successfully stored in Big Query!')

    def store_irrgs(self, irrgs_data):
        if irrgs_data:
            self._logger.info('Storing Irregularities data in Big Query...')

            table_id = self.__client.get_table(
                f'{self.__table_id_prefix}.{self.__city_prefix}_waze_data_irrgs'
            )

            insert_errors = self.__client.insert_rows(
                table_id, irrgs_data, row_ids=[None] * len(irrgs_data)
            )

            if insert_errors:
                self._logger.error(
                    f'Could not insert all rows from data, {len(insert_errors)} rows ignored'
                )
            else:
                self._logger.info(
                    'Irregularities data successfully stored in Big Query!'
                )

    def get_aggregated_jams_info(self):
        self._logger.info(
            'Retrieving aggregated jams and irregularities data by road segment of yesterday'
        )

        yesterday_date_str = (
            datetime.date.today() - datetime.timedelta(1.0)
        ).isoformat()

        query = f"""
            SELECT *
            FROM `{self.__table_id_prefix}.{self.__city_prefix}_waze_data_jams_agg_hour`
            WHERE DATE(georss_date) = '{yesterday_date_str}'
            ORDER BY georss_date ASC
        """

        result = self.__client.query(query).result()

        self._logger.info('Aggregated jams and irregularities info retrieved!')

        return result

    def get_aggregated_jams_durations_info(self):
        self._logger.info(
            'Retrieving aggregated duration data by road segment of yesterday'
        )

        yesterday_date_str = (
            datetime.date.today() - datetime.timedelta(1.0)
        ).isoformat()

        query = f"""
            SELECT *
            FROM `{self.__table_id_prefix}.{self.__city_prefix}_waze_data_jams_agg_times`
            WHERE DATE(start_ts) = '{yesterday_date_str}'
        """

        result = self.__client.query(query).result()

        self._logger.info('Aggregated jams and irregularities duration info retrieved!')

        return result
