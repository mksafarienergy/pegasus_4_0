import json
import requests
import time

import pandas as pd
from dateutil.relativedelta import relativedelta
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
        self.__password = 'SafariEnergy202$'
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
        self.__binsize = 'BinDay'
        self.__start_timestamp = ''
        self.__end_timestamp = ''
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


    def _set_binsize(self, hardware_id: str,  site_id: str, timeframe="5min", start_timestamp = None, end_timestamp = None ) -> None:
        """
        Test API call (call get_data_for_hardware )for binsize where you get production level data.
        Try first with 5 min, if error try 15min, and if error try 1Day.

        :param: hardware_id: the id of the hardware we are retrieving the data from
        :param: site_id: the id of the site we are retrieving data from
        :param: timeframe: the timeframe we want to use to retrieve data
        """
        try:
            if timeframe == '5min':
                log('Timeframe is set to 5min')
                self.__binsize = 'Bin5Min'
                if start_timestamp is None or end_timestamp is None: 
                    log(f'start_timestamp or end_timestamp is None')            
                    today_timestamp = get_todays_timestamp()
                    today_datetime = convert_timestamp_to_datetime(self.__end_timestamp)
                    one_month_ago = today_datetime - relativedelta(months=1)
                    last_month_timestamp = one_month_ago.strftime("%Y-%m-%dT%H:%M:%S")
                    self.__start_timestamp = last_month_timestamp
                    self.__end_timestamp = today_timestamp
                else:
                    self.__start_timestamp = start_timestamp 
                    self.__end_timestamp = end_timestamp
                hardware_data = self._get_data_for_hardware(hardware_id, site_id, self.__start_timestamp, self.__end_timestamp, 'KW') # type: ignore
                if "error" in hardware_data:
                    log(f'5min timeframe not available for {site_id}, moving on to 15min')

                    log(hardware_data)
                    timeframe = '15min'

            if timeframe == '15min':
                log('Timeframe is set to 15min')
                self.__binsize = 'Bin15Min'  
                if start_timestamp is None or end_timestamp is None:     
                    log(f'start_timestamp or end_timestamp is None')      
                    log(f'they are {start_timestamp, end_timestamp}')      
                    today_timestamp = get_todays_timestamp()
                    today_datetime = convert_timestamp_to_datetime(self.__end_timestamp)
                    one_month_ago = today_datetime - relativedelta(months=3)
                    last_3month_timestamp = one_month_ago.strftime("%Y-%m-%dT%H:%M:%S")
                    self.__start_timestamp = last_3month_timestamp
                    self.__end_timestamp = today_timestamp
                else:
                    self.__start_timestamp = start_timestamp 
                    self.__end_timestamp = end_timestamp
                hardware_data = self._get_data_for_hardware(hardware_id, site_id, self.__start_timestamp, self.__end_timestamp, 'KW') # it was KWHDel before # type: ignore
                if "error" in hardware_data:
                    log(f'15min timeframe not available for {site_id}, moving on to Day')                    
                    timeframe = 'Day'

            if timeframe == 'Day':
                log('Timeframe is set to Day')
                self.__binsize = 'BinDay'                  
                if start_timestamp is not None and end_timestamp is not None:    
                    self.__start_timestamp = start_timestamp
                    self.__end_timestamp = end_timestamp
                hardware_data = self._get_data_for_hardware(hardware_id, site_id, self.__start_timestamp, self.__end_timestamp, 'KW') # type: ignore
                if "error" in hardware_data:
                    log(f'1Day timeframe not available for {site_id}, throwing error')                    
                    raise Exception('cannot retrieve data for 5min, 15min, or day.')    


        except Exception as e:
            log(f'Error in _set_binsize: {e}')


    def __create_powertrack_session(self) -> None:
        self.__generate_tokens()

        self.__session = requests.Session()
        self.__session.headers.update(self.__headers)


    def _get_start_timestamp(self):
        """
        Returns the start_timestamp of PowertrackApi()
        """

        return self.__start_timestamp
    

    def _get_end_timestamp(self):
        """
        Returns the end_timestamp of PowertrackApi()
        """
        
        return self.__end_timestamp
    

    def _set_start_timestamp(self, timestamp):
        """
        Sets the start_timestamp of PowertrackApi()
        """
        
        self.__start_timestamp = timestamp
    

    def _set_end_timestamp(self, timestamp):
        """
        Sets the end_timestamp of PowertrackApi()
        """
        
        self.__end_timestamp = timestamp


    def _reset_binsize(self):
        """
        Reset __binsize of PowertrackApi() to Bin5Min
        """
        
        self.__binsize = 'Bin5Min'


    def _get_binsize(self):
        """
        get __binsize of PowertrackApi() 
        """
        
        return self.__binsize
    

    def __generate_tokens(self) -> None:
        """Generating initial session using login"""

        access_token_data = f'grant_type={self.__grant_type_password}&client_id={self.__client_id}&client_secret={self.__client_secret}&username={self.__username}&password={self.__password}'

        res = requests.request("POST", self.__auth_url, data=access_token_data, headers=self.__headers)
        res_data = json.loads(res.text)
        log(f'res_data is {res_data}')
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
        #log(f'binsize is {self.__binsize}')
        url = f'{self.__base_url}/v2/Data/BinData?'
        url += f'from={start_timestamp}'
        url += f'&to={end_timestamp}'
        url += f'&binSizes={self.__binsize}'
        url += '&tz=US/Eastern' 
        body = [{ 'hardwareId': hardware_id, 'siteId': site_id, 'fieldName': field_name, 'function': hw_function}]
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
