

import json
import requests
from src.wazedata import WazeData
from src.wazecartomodel import WazeCartoModel
from src.wazegeorss import WazeGeoRSS
from src.config import CARTO_API_KEY, CARTO_USER, WAZE_GEORSS

def carto_waze_lambda_handler(event, context):
    
    waze_georss = WazeGeoRSS(*WAZE_GEORSS)

    resp = requests.get(waze_georss.req_url, timeout=10)
    
    if (resp.status_code == requests.codes.ok):
        data = resp.json()
    
        if not data:
            data = {"msg": "no_data"}
        
        waze_data = WazeData(data)
        alerts_data = waze_data.build_alerts()
        jams_data = waze_data.build_jams()
        irrgs_data = waze_data.build_irrgs()
    
        waze_model = WazeCartoModel(CARTO_API_KEY, CARTO_USER)
        
        waze_model.store_alerts(alerts_data)
        waze_model.store_jams(jams_data)
        waze_model.store_irrgs(irrgs_data)
        
        response = {
            'statusCode': 200,
            'body': 'Function executed'
        }
        
        return response
    
    else:
        raise WazeRequestException('Request http error: {}'.format(resp.status_code))

