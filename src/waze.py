

class WazeGeoRSSException(Exception):
    pass


class WazeGeoRSS:
    
    def __init__(self, api_url, api_tkn, api_partner_name, api_frmt, api_types, api_poly):
        self.__api_url = api_url
        self.__api_tkn = api_tkn
        self.__api_partner_name = api_partner_name
        self.__api_frmt = api_frmt
        self.__api_types = api_types
        self.__api_poly = api_poly
        
        self.req_url = self.__get_waze_georss_url(self.__get_waze_georss_config())
    
    def __get_waze_georss_config(self, valid_types=['traffic','alerts','irregularities']):
        if not self.__api_frmt in ('JSON', 'XML'):
            raise WazeGeoRSSException("""
                You must provide a valid format (JSON | XML)
                """)
        
        if set(self.__api_types.split(',')).difference( set(valid_types)):
            raise WazeGeoRSSException("""
                You must provide valid types ({})
                """.format(','.join(valid_types)))
        
        return (
            self.__api_url,
            self.__api_tkn,
            self.__api_partner_name,
            self.__api_frmt,
            self.__api_types,
            self.__api_poly
        )
        
    @staticmethod
    def __get_waze_georss_url(api_config):
        req_url = '{0}?tk={1}&ccp_partner_name={2}&format={3}&types={4}&polygon={5}'
        return req_url.format(*api_config)