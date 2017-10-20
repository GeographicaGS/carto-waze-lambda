
from src.cartomodel import CartoModel


class WazeCartoModel(CartoModel):

    def __init__(self, carto_api_key, carto_user):
        super(WazeCartoModel, self).__init__(carto_api_key, carto_user)

        self.__logger = self.loadLogger(True)

    def store_alerts(self, alerts_data):
        
        alerts_vl = [
            """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
            '{country}','{city}','{reportdescription}',{confidence},{reportrating},
            {reliability},'{date}'::timestamp,E'{street}',{roadtype},{magvar},
            {nthumbsup},'{type}','{subtype}','{uuid}','{jam_uuid}')    
            """.format(**alert) for alert in alerts_data
        ]

        sql = """
            INSERT INTO waze_data_alerts
            (the_geom,country, city, reportdescription, confidence, reportrating,
             reliability, date, street, roadtype, magvar, nthumbsup, type,
             subtype, uuid, jam_uuid)
            VALUES {0}
            """.format(','.join(alerts_vl).replace('None','NULL'))

        self.query(sql, write_qry=True)
    
    def store_jams(self, jams_data):
        
        jams_vl = [
            """(ST_SetSRID(ST_GeomFromGeoJSON('{the_geom}'), 4326),
            '{country}','{city}',{speed},{length},{level},{delay},
            '{date}'::timestamp,E'{startnode}',E'{endnode}',E'{street}',
            {roadtype},'{type}','{turntype}','{uuid}','{blockingalert_uuid}')    
            """.format(**jam) for jam in jams_data
        ]

        sql = """
            INSERT INTO waze_data_jams
            (the_geom, country, city, speed, length, level, delay,
             date, startnode, endnode, street, roadtype, type, turntype, uuid,
             blockingalert_uuid)
            VALUES {0}
            """.format(','.join(jams_vl).replace('None','NULL'))

        self.query(sql, write_qry=True)
    
    def store_irrgs(self, irrgs_data):
        pass
        
