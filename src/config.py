"""
Carto Waze Lambda Connector

Developed by Geographica, 2017-2018.
"""

import os


class Config:
    """
    Configuration parameters:
        - Carto API.
        - WAZE API
        - Traffico
    """

    # Carto API
    CARTO_API_KEY = os.environ.get('CARTO_API_KEY')
    CARTO_USER = os.environ.get('CARTO_USER')

    # Waze API
    _WAZE_API_URL = os.environ.get('WAZE_API_URL')
    _WAZE_TKN = os.environ.get('WAZE_TKN')
    _WAZE_PARTNER_NAME = os.environ.get('WAZE_PARTNER')
    _WAZE_FRMT = 'JSON'
    _WAZE_TYPES = 'traffic,alerts,irregularities'
    _WAZE_POLY = os.environ.get('WAZE_POLY')
    WAZE_GEORSS = (
        _WAZE_API_URL,
        _WAZE_TKN,
        _WAZE_PARTNER_NAME,
        _WAZE_FRMT,
        _WAZE_TYPES,
        _WAZE_POLY
        )

    # Traffico
    TRAFFICO_PREFIX = os.environ.get('TRAFFICO_PREFIX')
