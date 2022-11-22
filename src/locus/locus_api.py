import json
import requests
import time


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
        self.__session = None
        self.__access_token = None
        self.__refresh_token = None
        self.__create_locus_session()


    def use_locus_session(self, url) -> requests.Response:
        """We should only ever GET from locus"""

        print(self.__access_token)
        print(self.__refresh_token)
        print(self.__session)

        res = self.__session.get(url)
        res_data = json.loads(res.text)
        if res_data['statusCode'] == 429:
            # Too many requests
            #  pause for a min
            time.sleep(60)
            res = self.__session.get(url)
            res_data = json.loads(res.text)
        elif res_data['statusCode'] == 401:
            if self.__refresh_token != None:
                self.__refresh_session()
            else:
                self.__create_locus_session()
            res = self.__session.get(url)
            res_data = json.loads(res.text)

        return res_data


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


    # def locus_url_builder(self, site_id)
