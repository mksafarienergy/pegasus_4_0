import json
import requests
import time
import pandas as pd

#from utils import *
#from src.logger.logger import *


class SolarEdgeApi():
    """Authentication & session usage for Solar Edge """

    def __init__(self):
        self.__api_key = "XUR0SMGADU7Y2NY4M2R7ZOS3KJ5WXSR4"
        self.__url = "https://monitoringapi.solaredge.com" 
        self.__api_stats = pd.DataFrame(columns=['url', 'run_time'])

    def _construct_response(self, inner_url) -> requests.Response:
        """
        given an inner_url, the method constructs an endpoint_url and returns a response. 

        :param inner_url: the middle part of the url that directs to a specific api.
        :return: requests.Response (the response of the request)
        """

        endpoint_url = (self.__url
                        +inner_url+'api_key='+self.__api_key)  
        print(f'endpoint url is {endpoint_url}')
        response = requests.get(endpoint_url)
        if response.status_code == 200:
            print("request.get was a success")
        elif response.status_code == 429:
            print("too many requests")
            time.sleep(60) 
            response = requests.get(endpoint_url)
        elif response.status_code == 400:
            print("something went wrong on the inputs")
        elif response.status_code == 403:
            print("the endpoint_url is incorrect")
        elif response.status_code == 404:
            print("the request data cannot be found")
        return response 
    
    def _get_site_details(self, site_id) -> list[dict]:
        """
        returns a list of dictionaries of site details and its values 
        
        :param site_id: the id of the site
        """

        inner_url = (
            f'/site/{site_id}/details?'
        )
        response = self._construct_response(inner_url)
        data = json.loads(response.text)  
        formatted_data = data['details']
        return formatted_data

    def _get_energy_for_site(self, site_id, start_date, end_date, time_unit) -> list[dict]:
        """
        given an id or multiple id, separated by commas, start_date, end_date, time_unit,
        return energy production as a list of dictionaries for the given timeframe. 

        :param id: the site_id we want to get data from. Could get multiple Id as well, the format is 'id1,id2,...'
        :param start_date: the starting date of the timeframe. The format is Year,month,day
        :param end_date: the end date of the timeframe. The format is Year,month,day
        :param time_unit: (This is the timeseries that the API returns the data in. The accepted 
        input are: QUARTER_OF_AN_HOUR, HOUR, DAY, WEEK, MONTH, YEAR)

        Usage limitation: (This API is limited to one year when using timeUnit=DAY (i.e., daily resolution) 
        and to one month when using timeUnit=QUARTER_OF_AN_HOUR or timeUnit=HOUR. This means that the period
        between endTime and startTime should not exceed one year or one month respectively. If the period is longer,
        the system will generate error 403 with proper description.
        """

        inner_url = (
            f'/sites/{site_id}/energy?timeUnit={time_unit}'
            f'&startDate={start_date}&endDate={end_date}&'
        )
        response=self._construct_response(inner_url)
        data=json.loads(response.text)      
        formatted_data=data['sitesEnergy']['siteEnergyList']
        return formatted_data
    
    def _get_site_components(self, site_id) -> list[dict]:
        """
        Returns a list of all the components for a specific site
        
        :param site_id: the id of the site
        """

        inner_url = (
            f'/equipment/{site_id}/list?'
        )
        response=self._construct_response(inner_url)
        data=json.loads(response.text)  
        formatted_data=data['reporters']['list']
        return formatted_data
    
    def _get_inverter_data(self, site_id, serial_number, start_timestamp, end_timestamp) -> list[dict]:
        """
        Returns all the inverter data for the requested site, within the requested timeframe. 
        
        :param site_id: the id of the site
        :param serial_number: the serial number of the inverter. 
        :param start_timestamp: the starting timestamp. Format is yyyy-mm-dd hh:mm:ss 
        :param end_timestamp: the ending timestamp. Format is yyyy-mm-dd hh:mm:ss 

        Usage limitation: (This API is limited to one-week period. This means that the period
        between endTime and startTime should not exceed one week. If the period is longer, the 
        system will generate error 403 with proper description. Furthermore, the timeframe it 
        returns in is every 5 minutes.)
        """

        inner_url=(
            f'/equipment/{site_id}/{serial_number}/data?startTime={start_timestamp}'
            f'&endTime={end_timestamp}&'
        )
        response = self._construct_response(inner_url)
        data=json.loads(response.text) 
        print(data)
        formatted_data=data['data']['telemetries'][0]
        return formatted_data
    
    def _get_meters_lifetime_data(self, site_id, start_timestamp, end_timestamp, time_unit = None, meters = None) -> list[dict]:
        """
        Returns for each meter on site its lifetime energy reading, metadata and the device to which it's connected to.

        :param site_id: the id of the site on solar edge 
        :param start_timestamp: the starting timestamp. Format is yyyy-mm-dd hh:mm:ss 
        :param end_timestamp: the ending timestamp. Format is yyyy-mm-dd hh:mm:ss 
        :param time_unit: (This is an optional parameter. This is the timeseries that the API returns the data in. The accepted 
        input are: QUARTER_OF_AN_HOUR, HOUR, DAY, WEEK, MONTH, YEAR). Default is DAY. 

        Usage limitation: (This API is limited to one year when using timeUnit=DAY (i.e., daily resolution) 
        and to one month when using timeUnit=QUARTER_OF_AN_HOUR or timeUnit=HOUR. This means that the period
        between endTime and startTime should not exceed one year or one month respectively. If the period is longer,
        the system will generate error 403 with proper description.
        """

        if time_unit is None: 
            inner_url = (
            f'/site/{site_id}/meters?meters=Consumption&startTime={start_timestamp}'
            f'&endTime={end_timestamp}&'
            )
        else:
            inner_url = (
                f'/site/{site_id}/meters?meters=Production,Consumption&startTime={start_timestamp}&timeUnit={time_unit}'
                f'&endTime={end_timestamp}&'
            )
        response=self._construct_response(inner_url)
        data=json.loads(response.text)  
        # formatted_data=data['reporters']['list']
        print(data)
        return data
    
    def _get_sensor_data(self, site_id, start_timestamp, end_timestamp) -> list[dict]:
        """
        Returns all the sensor data for the requested site, with the requested timeframe. 
        
        :param site_id: the id of the site
        :param start_timestamp: the starting timestamp. Format is yyyy-mm-dd hh:mm:ss 
        :param end_timestamp: the ending timestamp. Format is yyyy-mm-dd hh:mm:ss 

        Usage limitation: (This API is limited to one-week period. This means that the period 
        between endDate and startDate should not exceed one week. If the period is longer, the 
        system will generate error 403 with a description. Furthermore, the timeframe it returns in is every 5 minutes.)
        """

        inner_url=(
            f'/site/{site_id}/sensors?startDate={start_timestamp}'
            f'&endDate={end_timestamp}&'
        )
        response = self._construct_response(inner_url)
        data=json.loads(response.text) 
        formatted_data=data['siteSensors']['data'][0]['telemetries']
        return formatted_data

solar_edge = SolarEdgeApi()
#solar_edge._get_site_components('2256292')
#id='2256292,2184009'
id=1129227
#solar_edge._get_site_details(id)
#solar_edge._get_energy_for_site(id,'2023-03-12','2023-03-14','DAY')
#solar_edge._get_site_components('2256292')
# poai=solar_edge._get_sensor_data(2184009,'2023-03-13 00:00:00','2023-03-13 23:59:59')
#solar_edge._get_meters_lifetime_data(2256292, '2023-03-01 00:00:00', '2023-03-13 23:59:59', 'HOUR')
solar_edge._get_inverter_data(2256292, '7E09D12D-85', '2023-03-13 11:00:00', '2023-03-13 13:00:00')


