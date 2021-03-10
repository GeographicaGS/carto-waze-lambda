"""
Carto Waze Lambda Connector

Developed by Geographica, 2017-2018.
"""

from src.config import Config
from src.models.waze_bq_model import WazeBigQueryModel
from src.models.wazecartomodel import WazeCartoModel


def carto_waze_daily_agg_handler(event, context):
    waze_carto_model = WazeCartoModel(
        Config.CARTO_API_KEY,
        Config.CARTO_USER,
        Config.TRAFFICO_PREFIX,
        Config.CARTO_MAX_HOURS_DATA_RETENTION,
    )

    waze_bq_model = WazeBigQueryModel(
        Config.BIG_QUERY_HISTORIC_PROJECT,
        Config.BIG_QUERY_HISTORIC_DATASET,
        Config.TRAFFICO_PREFIX,
    )

    agg_jams = waze_bq_model.get_aggregated_jams_info()
    waze_carto_model.store_aggregated_jams_by_hour(agg_jams)

    agg_durations = waze_bq_model.get_aggregated_jams_durations_info()
    waze_carto_model.store_aggregated_jams_durations(agg_durations)
