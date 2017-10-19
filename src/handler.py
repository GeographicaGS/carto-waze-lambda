

import json
import requests
from src.carto import CartoModel
from src.waze import WazeGeoRSS
from src.config import CARTO_API_KEY, CARTO_USER, WAZE_GEORSS

def carto_waze_lambda_handler(event, context):
    
    waze_georss = WazeGeoRSS(*WAZE_GEORSS)

    resp = requests.get(waze_georss.req_url, timeout=10)
    
    if (resp.status_code == requests.codes.ok):
        data = resp.json()
    
        cm = CartoModel(CARTO_API_KEY, CARTO_USER)
    
        if not data:
            data = {"msg": "no_data"}
    
        json_data = json.dumps(data, ensure_ascii=False)
        json_data = json_data.replace("'", "\\'")
    
        sql = """
            INSERT INTO waze_data_test (waze_json)
                VALUES (E'{0}'::json);
            """.format(json_data)
    
        cm.query(sql, write_qry=True)
        
        response = {
            'statusCode': 200,
            'body': 'Function executed'
        }
        
        return response
    
    else:
        raise WazeRequestException('Request http error',format(resp.status_code))

