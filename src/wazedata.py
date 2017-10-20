import json
from datetime import datetime


class WazeData:
    """
    Waze data model according to "Waze Traffic-data 
    Specification Document (Version 2.7.2)".
    
    Waze traffic data model features:
        - Alerts.
        - Jams.
        - irregularities.
    """
    
    def __init__(self, data):
        self.__data = data
    
    @staticmethod
    def __build_geojson_line(line):
        return {
            "coordinates": [[pt.get('x'), pt.get('y')] for pt in line], 
            "type": "LineString"
            }
    
    @staticmethod
    def __build_geojson_point(point):
        return {
            "coordinates": [point.get('x'), point.get('y')], 
            "type": "Point"
            }
    
    @staticmethod
    def __build_timestamp(milsecs, tm_frmt='%Y-%m-%dT%H:%M:%S.%fZ'):
        secs = milsecs / 1000.0
        return datetime.utcfromtimestamp(secs).strftime(tm_frmt)
        
    @staticmethod
    def __format_street_names(street_name):
        if street_name and isinstance(street_name, str):
            return street_name.replace("'", "\\'")
        
    @staticmethod
    def __quote_list(data):
        return ["'{0}'".format(dt) for dt in data]
    
    @property
    def get_time_range(self):
        start_time = self.__build_timestamp(self.__data.get('startTimeMillis'))
        end_time = self.__build_timestamp(self.__data.get('endTimeMillis'))
        return {
            'start_time': start_time,
            'end_time': end_time
            }
    
    def __get_raw_alerts(self):
        return self.__data.get('alerts')

    def __get_raw_jams(self):
        return self.__data.get('jams')

    def __get_raw_irrgs(self):
        return self.__data.get('irregularities')

    def build_alerts(self):
        alerts_raw = self.__get_raw_alerts()
        
        if alerts_raw:
            alerts = []
            
            for alert in alerts_raw:
                geom_pt = alert.get('location')
                
                date_wz = alert.get('pubMillis')
                tm_rng = self.get_time_range
                
                alerts.append({
                    'the_geom': json.dumps(self.__build_geojson_point(geom_pt)),
                    'date': self.__build_timestamp(date_wz),
                    'country': alert.get('country'),
                    'city': alert.get('city'),
                    'reportdescription': alert.get('reportDescription'),
                    'confidence': alert.get('confidence'),
                    'reportrating': alert.get('reportRating'),
                    'reliability': alert.get('reliability'),
                    'street': self.__format_street_names(alert.get('street')),
                    'roadtype': alert.get('roadType'),
                    'magvar': alert.get('magvar'),
                    'nthumbsup': alert.get('nThumbsUp'),
                    'type': alert.get('type'),
                    'subtype': alert.get('subType'),
                    'uuid': alert.get('uuid'),
                    'jam_uuid':  alert.get('jamUuid'),
                    'georss_date': tm_rng.get('start_time')
                }) 
            
            return alerts

    def build_jams(self):
        jams_raw = self.__get_raw_jams()
        
        if jams_raw:
            jams = []
            
            for jam in jams_raw:
                geom_ln = jam.get('line')
                
                date_wz = jam.get('pubMillis')
                tm_rng = self.get_time_range
              
                jams.append({
                    'the_geom': json.dumps(self.__build_geojson_line(geom_ln)),
                    'date': self.__build_timestamp(date_wz),
                    'country': jam.get('country'),
                    'city': jam.get('city'),
                    'speed': jam.get('speed'),
                    'length': jam.get('length'),
                    'level': jam.get('level'),
                    'delay': jam.get('delay'),
                    'startnode': self.__format_street_names(jam.get('startNode')),
                    'endnode': self.__format_street_names(jam.get('endNode')),
                    'street': self.__format_street_names(jam.get('street')),
                    'roadtype': jam.get('roadType'),
                    'type': jam.get('type'),
                    'turntype': jam.get('turnType'),
                    'uuid': jam.get('uuid'),
                    'blockingalert_uuid':  jam.get('blockingAlertUuid'),
                    'georss_date': tm_rng.get('start_time')
                }) 
            
            return jams

    def build_irrgs(self):
        irrgs_raw = self.__get_raw_irrgs()
        
        if irrgs_raw:
            irrgs = []
            
            for irrg in irrgs_raw:
                geom_ln = irrg.get('line')
                
                dt_date_wz = irrg.get('detectionDateMillis')
                up_date_wz = irrg.get('updateDateMillis')
                tm_rng = self.get_time_range
                
                alerts_arr = irrg.get('alerts')
                if alerts_arr:
                    alerts_uuid = ','.join(self.__quote_list(
                        [al.get('uuid') for al in alerts_arr]
                        )
                    )
                else:
                    alerts_uuid = None
              
                irrgs.append({
                    'the_geom': json.dumps(self.__build_geojson_line(geom_ln)),
                    'detectiondate': self.__build_timestamp(dt_date_wz),
                    'updatedate': self.__build_timestamp(up_date_wz),
                    'alertscount': irrg.get('alertsCount'),
                    'alerts_uuid': alerts_uuid,
                    'country': irrg.get('country'),
                    'city': irrg.get('city'),
                    'speed': irrg.get('speed'),
                    'regularspeed': irrg.get('regularSpeed'),
                    'length': irrg.get('length'),
                    'jamlevel': irrg.get('jamlevel'),
                    'severity': irrg.get('severity'),
                    'highway': irrg.get('highway'),
                    'trend': irrg.get('trend'),
                    'seconds': irrg.get('seconds'),
                    'delayseconds': irrg.get('delaySeconds'),
                    'startnode': self.__format_street_names(irrg.get('startNode')),
                    'endnode':  self.__format_street_names(irrg.get('endNode')),
                    'street':  self.__format_street_names(irrg.get('street')),
                    'type': irrg.get('type'),
                    'id': irrg.get('id'),
                    'ncomments': irrg.get('nComments'),
                    'nimages': irrg.get('nImages'),
                    'nthumbsup': irrg.get('nThumbsUp'),
                    'georss_date': tm_rng.get('start_time')
                }) 
            
            return irrgs
            
