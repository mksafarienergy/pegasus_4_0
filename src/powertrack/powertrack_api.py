import json
import requests
import time

import pandas as pd

from utils import *
from src.logger.logger import *

class PowertrackApi():
    """Authentication & session usage for powertrack """

    def __init__(self):
        """TODO: Look into having these values in some azure vault"""
        self.__auth_url = 'https://api.alsoenergy.com/Auth/token'
        self.__base_url = 'https://api.alsoenergy.com'
        self.__grant_type_password = 'password'
        self.__grant_type_refresh = 'refresh_token'
        self.__username = 'mkajoshaj@aspenpower.com'
        self.__password = 'SafariEnergy202#'
        self.__client_id = '5b7bc63684c19388e1d253565cb99980'
        self.__client_secret = 'c653512c7190315d6ecf008da9ee3f33'
        self.__headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache",
        }
        self.__session = requests.session()
        self.__access_token = None
        self.__refresh_token = None
        self.__create_powertrack_session()
        self.__api_stats = pd.DataFrame(columns=['url', 'run_time'])
        log(self.__access_token)
        log(self.__refresh_token)
        log('Constructor finished for PowertrackApi')


    def _deconstructor(self):
        """
        Deconstructor - Exporing dfs to csvs
        __del__ does not work well here because it seems that the imports are 
            destroyed before it runs. Making the functions below throw an error
        """

        now = get_time_now()
        self.__api_stats.to_csv(f'csvs/api_stats_{now}.csv')
        log('Deconstructor for PowertrackApi')


    def __create_powertrack_session(self) -> None:
        self.__generate_tokens()

        self.__session = requests.Session()
        self.__session.headers.update(self.__headers)


    def __generate_tokens(self) -> None:
        """Generating initial session using login"""

        access_token_data = f'grant_type={self.__grant_type_password}&client_id={self.__client_id}&client_secret={self.__client_secret}&username={self.__username}&password={self.__password}'

        res = requests.request("POST", self.__auth_url, data=access_token_data, headers=self.__headers)
        res_data = json.loads(res.text)
        self.__access_token = res_data['access_token']

        self.__headers["Authorization"] = f'Bearer {self.__access_token}'
        

    def __use_session_get(self, url: str) -> dict:
        """We should only ever GET from powertrack"""

        log(f'Endpoint: {url}')
        start = time.time()
        res = self.__session.get(url)
        res_json = json.loads(res.text)
        if 'statusCode' in res_json:
            if res_json['statusCode'] == 429:
                # Too many requests, pause for a minute
                time.sleep(60)
                res = self.__session.get(url)
                res_json = json.loads(res.text)
            elif res_json['statusCode'] == 401:
                self.__create_powertrack_session()
                res = self.__session.get(url)
                res_json = json.loads(res.text)

        run_time = time.time() - start
        self.__api_stats = self.__api_stats.append({ 'url': url, 'run_time': run_time }, ignore_index=True)
        log(f'Request responded with a {res} status code and took {run_time} seconds to run')
        return res_json
    

    def __use_session_post(self, url: str, body: list[dict], header=None) -> dict:
        """"""
        log(f'Endpoint: {url}')
        start = time.time()
        if header:
            res = self.__session.post(url, data=str(body),headers=header)
        else:
            res = self.__session.post(url, data=str(body))
        log(f'powertrack.py, __use_session_post: response is {res}')
        res_json = json.loads(res.text)

        if 'statusCode' in res_json:
            if res_json['statusCode'] == 429:
                # Too many requests, pause for a minute
                time.sleep(60)
                res = self.__session.get(url)
                res_json = json.loads(res.text)
            elif res_json['statusCode'] == 401:
                self.__create_powertrack_session()
                res = self.__session.get(url)
                res_json = json.loads(res.text)

        run_time = time.time() - start
        self.__api_stats = self.__api_stats.append({ 'url': url, 'run_time': run_time }, ignore_index=True)
        log(f'Request responded with a {res} status code and took {run_time} seconds to run')
        return res_json


    def _get_data_for_site(self, site_id: str , intervaled_timestamps: list[list[str]], short_name: str) -> list:
        """
        Iterates through the split_timestamps list and gets the first and last elements of the 
            current iteration to make start and end dates. Then hits an endpoint to get data 
            for that site

        :param site_id: site id
        :param timestamps: list of timestamps starting from PTO to today
                                with intervals of 1500 inbetween
                                i.e. ['2018-10-14T00:00:00', '2022-11-22T00:00:00', ...]
        :param short_name: the specific datapoint we are trying to get
        :return: list of dicts (responses from the endpoint)
        """

        responses = []

        for timestamps in intervaled_timestamps:
            start_date = timestamps[0]
            end_date = add_days_to_date(timestamps[-1], 1)
            url = f'{self.__base_url}/sites/{str(site_id)}/data?'
            url += f'start={start_date}'
            url += f'&end={end_date}'
            url += '&tz=UTC&gran=daily'
            url += f'&fields={short_name}'

            response = self.__use_session_get(url)
            responses.extend(response['data'])

        return responses


    def _get_site_hardware(self, site_id: str) -> list:
        """Gets all the components for a specific site"""

        url = f'{self.__base_url}/sites/{str(site_id)}/Hardware'

        response = self.__use_session_get(url)
        return response['hardware']
        #return response['components']

    def _get_site_hardware_details(self, hardwareid: str):
        """Gets all the components for a specific site"""

        url = f'{self.__base_url}/Hardware/{str(hardwareid)}'

        response = self.__use_session_get(url)
        return response
    
    def _get_site_details(self, site_id: str):
        """Gets all the components for a specific site"""

        url = f'{self.__base_url}/Sites/{str(site_id)}'

        response = self.__use_session_get(url)
        return response

    
    def _get_data_for_hardware(self, hardware_id: str, site_id: str, start_timestamp: str, end_timestamp: str, field_name: str, hw_function: str = 'Diff'):
        """
        Iterates through the split_timestamps list and gets the first and last elements of the 
            current iteration to make start and end dates. Then hits an endpoint to get data 
            for that site

        :param site_id: site id
        :param intervaled_timestamps: list of list of timestamps starting from PTO (ideally) to today
                                    with intervals inbetween
                                    i.e. [['2018-10-14T00:00:00', '2022-11-22T00:00:00'], [str, str], ...]
        :param short_name: the specific datapoint we are trying to get
        :return: list of dicts (responses from the endpoint)
        """
        responses = []

        # for timestamps in intervaled_timestamps:
        url = f'{self.__base_url}/v2/Data/BinData?'
        url += f'from={start_timestamp}'
        url += f'&to={end_timestamp}'
        url += '&binSizes=BinDay'
        url += '&tz=utc' 
        body = [{ 'hardwareId': hardware_id, 'siteId': site_id, 'fieldName': field_name, 'function': hw_function }]
        header = {"Content-Type": "application/json"}
        log(f'powertrack_api.py, _get_data_for_hardware: body is {body}')
        response = self.__use_session_post(url, body, header)
        return response


    def _get_data_available_for_component(self, component_id: str) -> list[dict]:
        """
        Getting the data available for a specific component from powertrack
            Data will include basefields like shortname, longname, etc.

        :param component_id: id of the desired component
        :return: list of dictionaries. Each element will contain a basefield
                that holds information of a specific piece of information 
                available for the component
                ex. {"baseField":"Hz",
                    "longName":"AC Frequency",
                    "source":"Measured",
                    "unit":"Hz",
                    "aggregations": []... etc.}
        """

        url = f'{self.__base_url}/components/{component_id}/dataavailable'

        response = self.__use_session_get(url)
        return response['baseFields']


    def _get_site_alerts(self, site_id:str) -> list[dict]:
        """"""
        url = f'{self.__base_url}/sites/{site_id}/alerts?tz=UTC'

        response = self.__use_session_get(url)
        log(response)
        return response


    def _testme(self):
        """Used to test out the functions above"""

        timestamps = get_timestamps(datetime(2022, 11, 10))
        # test1 = self._get_data_for_site('3822685', timestamps, 'Wh_sum')
        # test2 = self._get_site_components('3822685')
        # test3 = self._get_data_for_component('2590683', timestamps, 'Wh_sum')
        test4 = self._get_data_available_for_component('2590683')

        # print(test1)
        # print(test2)
        # print(test3)
