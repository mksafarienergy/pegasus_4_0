import json
import requests
import time

from utils import *

class LocusApi():
    """Authentication & session usage for locus """

    def __init__(self):
        """TODO: Look into having these values in some azure vault"""
        self.__auth_url = "https://api.locusenergy.com/oauth/token"
        self.__grant_type_password = 'password'
        self.__grant_type_refresh = 'refresh_token'
        self.__username = 'pperillo@safarienergy.com'
        self.__password = 'Locus123'
        self.__client_id = '5d34704c55b8a82b119c1b96418faada'
        self.__client_secret = '5c628e02730d2d776263880c8b22c3a6'
        self.__headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache",
        }
        self.__session = requests.session()
        self.__access_token = None
        self.__refresh_token = None
        self.__create_locus_session()


    def __create_locus_session(self):
        self.__generate_tokens()

        self.__session = requests.Session()
        self.__session.headers.update(self.__headers)


    def __generate_tokens(self):
        """Generating initial session using login"""

        data = f'grant_type={self.__grant_type_password}&client_id={self.__client_id}&client_secret={self.__client_secret}&username={self.__username}&password={self.__password}'

        res = requests.request("POST", self.__auth_url, data=data, headers=self.__headers)
        res_data = json.loads(res.text)
        
        self.__access_token = res_data['access_token']
        self.__refresh_token = res_data['refresh_token']
        self.__headers["Authorization"] = f'Bearer {self.__access_token}'
        

    def __refresh_session(self):
        """Refreshing session using refresh token"""

        data = f'grant_type={self.__grant_type_refresh}&client_id={self.__client_id}&client_secret={self.__client_secret}&refresh_token={self.__refresh_token}'
        
        res = requests.request("POST", self.__auth_url, data=data, headers=self.__headers)
        res_data = json.loads(res.text)
        
        self.__access_token = res_data['access_token']
        self.__headers["Authorization"] = f'Bearer {self.__access_token}'
        self.__session.headers.update(self.__headers)


    def __use_session(self, url) -> dict:
        """We should only ever GET from locus"""

        res = self.__session.get(url)
        res_json = json.loads(res.text)
        if res_json['statusCode'] == 429:
            # Too many requests
            #  pause for a min
            time.sleep(60)
            res = self.__session.get(url)
            res_json = json.loads(res.text)
        elif res_json['statusCode'] == 401:
            if self.__refresh_token != None:
                self.__refresh_session()
            else:
                self.__create_locus_session()
            res = self.__session.get(url)
            res_json = json.loads(res.text)

        print(res_json['statusCode'])
        return res_json


    def get_data_for_site(self, site_id: str , timestamps: list[str], short_name: str) -> list:
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

        i, j = 0, 1
        while (j < len(timestamps)):
            start_date = timestamps[i]
            end_date = timestamps[j] #add_days_to_date(timestamps[-1], 1)
            url = f'https://api.locusenergy.com/v3/sites/{str(site_id)}/data?'
            url += f'start={start_date}'
            url += f'&end={end_date}'
            url += '&tz=UTC&gran=daily'
            url += f'&fields={short_name}'

            response = self.__use_session(url)
            responses.extend(response['data'])
            i += 1
            j += 1

        return responses
