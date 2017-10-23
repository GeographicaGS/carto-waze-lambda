

import re
from carto.auth import APIKeyAuthClient
from carto.sql import SQLClient
from src.logger import Logger


class CartoModelException(Exception):
    pass

class CartoModel:

    def __init__(self, carto_api_key, carto_user, verbose=True):
        self.__carto_api_key = carto_api_key
        self.__carto_user = carto_user

        self.__logger = self.load_logger(verbose)

    def load_logger(self, verbose):
        if not verbose:
            lg = Logger(level='ERROR')
        else:
            lg = Logger()

        return lg.get()

    @staticmethod
    def __get_auth_client(api_key, carto_user):
        cartouser_url = "https://{0}.carto.com".format(carto_user)
        return APIKeyAuthClient(cartouser_url, api_key)

    @staticmethod
    def __is_write_query(sql_query):
        writeCmds = 'drop|delete|insert|update|grant|execute|perform|create|begin|commit|alter'
        isWrite = re.search(writeCmds, sql_query.lower())
        if isWrite:
            return True

    def query(self, sql_query, parse_json=True, do_post=True, format=None, write_qry=False):
        try:
            if not write_qry and self.__is_write_query(sql_query):
                raise CartoModelException("Aborted query. No write queries allowed.")

            auth_client = self.__get_auth_client(self.__carto_api_key, self.__carto_user)
            sql = SQLClient(auth_client, api_version='v2')

            res = sql.send(sql_query, parse_json, do_post, format)
            return res['rows']

        except Exception as err:
            self.__logger.error("Error sending query to Carto: {0}\n{1}".format(err, sql_query))
            raise CartoModelException(err)
