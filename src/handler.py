"""
Carto Waze Lambda Connector

Developed by Geographica, 2017-2018.
"""

import requests

from src.config import Config
from src.logger import Logger
from src.models.waze_bq_model import WazeBigQueryModel
from src.models.wazecartomodel import WazeCartoModel
from src.wazedata import WazeData
from src.wazegeorss import WazeGeoRSS, WazeGeoRSSException


def carto_waze_lambda_handler(event, context):

    lg = Logger(level='INFO')
    logger = lg.get()

    waze_georss = WazeGeoRSS(*Config.WAZE_GEORSS)

    resp = requests.get(waze_georss.req_url, timeout=10)

    if resp.status_code == requests.codes.ok:
        data = resp.json()

        if not data:
            data = {"msg": "no_data"}

        waze_data = WazeData(data)
        waze_time = waze_data.get_time_range
        waze_st = waze_time.get('start_time')
        alerts_data = waze_data.build_alerts()
        jams_data = waze_data.build_jams()
        irrgs_data = waze_data.build_irrgs()

        waze_carto_model = WazeCartoModel(
            Config.CARTO_API_KEY,
            Config.CARTO_USER,
            Config.TRAFFICO_PREFIX,
            Config.CARTO_MAX_HOURS_DATA_RETENTION,
        )
        waze_carto_model.store_alerts(alerts_data)
        waze_carto_model.store_jams(jams_data)
        waze_carto_model.store_irrgs(irrgs_data)

        waze_carto_model.refresh_mviews()

        if Config.BIG_QUERY_ENABLE_HISTORIC:
            waze_bq_model = WazeBigQueryModel(
                Config.BIG_QUERY_HISTORIC_PROJECT,
                Config.BIG_QUERY_HISTORIC_DATASET,
                Config.TRAFFICO_PREFIX,
            )
            waze_bq_model.store_alerts(alerts_data)
            waze_bq_model.store_jams(jams_data)

            irrgs_data = waze_data.build_irrgs(alerts_array_as_str=False)
            waze_bq_model.store_irrgs(irrgs_data)

        response = {
            'statusCode': 200,
            'body': 'Function executed. GEORSS date: {}'.format(waze_st),
        }

        logger.info(response)

        return response

    else:
        msg = 'Request http error: {}'.format(resp.status_code)
        raise WazeGeoRSSException(msg)
