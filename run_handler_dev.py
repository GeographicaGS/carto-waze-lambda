"""
RUN Handler. Only for development purpose
"""

import argparse

from src.daily_agg_handler import carto_waze_daily_agg_handler
from src.handler import carto_waze_lambda_handler


_GEORSS_HANDLER = 'georss'
_DAILY_AGGS_HANDLER = 'daily-aggs'


def _get_handler_name():
    parser = argparse.ArgumentParser(
        description='Launcher for Serverless handlers for debugging purposes'
    )

    parser.add_argument(
        'handler_name',
        choices=[_GEORSS_HANDLER, _DAILY_AGGS_HANDLER],
        help='Name of the handler to execute',
    )

    namespace = parser.parse_args()

    return namespace.handler_name


if __name__ == '__main__':
    handler_name = _get_handler_name()
    if handler_name == _DAILY_AGGS_HANDLER:
        carto_waze_daily_agg_handler('', '')
    else:
        carto_waze_lambda_handler('', '')
