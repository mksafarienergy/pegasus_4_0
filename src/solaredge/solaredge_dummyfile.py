import requests
import json
import pandas as pd


class SolarEdge:

    def __init__(self, sites) -> None:
        self.api_key = "api_key=XUR0SMGADU7Y2NY4M2R7ZOS3KJ5WXSR4"
        self.url = "https://monitoringapi.solaredge.com/" 

    def construct_response(self, inner_url) -> requests.Response:
        endpoint_url = (self.url
                        +inner_url+self.api_key)  
        print(f'endpoint url is {endpoint_url}')
        response = requests.get(endpoint_url)
        print(type(response))
        return response 
    
    def get_energy(self,id,start_date,end_date,time_unit):
        inner_url=(
            f'sites/{id}/energy?timeUnit={time_unit}'
            f'&startDate={start_date}&endDate={end_date}&'
        )
        response=self.construct_response(inner_url)
        data=json.loads(response.text)      
        formatted_data=data['sitesEnergy']['siteEnergyList']
        df=pd.json_normalize(formatted_data, meta='siteId', record_path=['energyValues','values'])
        return df
    
    def get_sensor_data(self,id,start_date,end_date):
        inner_url=f'site/{id}/sensors?startDate={start_date}%200:00:00&endDate={end_date}%2023:59:59&'
        response = self.construct_response(inner_url)
        data=json.loads(response.text) 
        formatted_data=data['siteSensors']['data'][0]['telemetries'][:]
        #df=pd.json_normalize(formatted_data,meta='date',record_path=['planeOfArrayIrradiance'])
        print(pd.json_normalize(formatted_data))
        print(data)
        return pd.json_normalize(formatted_data)

id="2256292,2184009"
solar_edge = SolarEdge(34)
df = solar_edge.get_energy(id,'2022-05-01','2022-06-01','DAY')
poai=solar_edge.get_sensor_data(2256292,'2023-03-13','2023-03-13')
print("End")

#Notes:
#For now just do daily timeframe even for Plane of array irradiance, but you need to construct the system where to get the hourly data you just need to change a field. 
#Now map it to locus api
#There are functions in utils that can help with timeframe. 
#api should only have functions that connect to the api and only look at the response to return 200 and it should have different functionalities to warn people
#solaredge_api shouldn't manipulate data at all, it should return the raw data, meaning response. 
#solaredge.py should manipulate the raw data and return an overall dataframe. 
#Keep the inputs hardcoded


#questions:
#Input


# api_key = "XUR0SMGADU7Y2NY4M2R7ZOS3KJ5WXSR4"


# # Endpoint URL for the SolarEdge API
# url_100 = "https://monitoringapi.solaredge.com/sites/list?api_key={api_key}"

# url_200 = "https://monitoringapi.solaredge.com/sites/list?startIndex=100&api_key={api_key}"

# # Replace {site_id} with your site ID
# site_id = "YOUR_SITE_ID_HERE"

# # Format the endpoint URL with the site ID and API key
# endpoint_url_100 = url_100.format(api_key=api_key)

# endpoint_url_200 = url_200.format(api_key=api_key)


# # Send a GET request to the SolarEdge API
# response_100 = requests.get(endpoint_url_100)

# # If the response was successful (status code 200), parse the JSON data
# if response_100.status_code == 200:
#     data_100 = json.loads(response_100.text)
#     df_100 = pd.json_normalize(data_100)
#     site_100= data_100['sites']['site']
#     df_site_100 = pd.json_normalize(site_100)
#     print(data_100)
# else:
#     print("Error connecting to SolarEdge API")



# response_200 = requests.get(endpoint_url_200)

# # If the response was successful (status code 200), parse the JSON data
# if response_200.status_code == 200:
#     data_200 = json.loads(response_200.text)
#     df_200 = pd.json_normalize(data_200)
#     site_200= data_200['sites']['site']
#     df_site_200 = pd.json_normalize(site_200)
#     print(df_site_200)
# else:
#     print("Error connecting to SolarEdge API")


def long_function_name(
        var_one, var_two, var_three,
        var_four):
    print(var_one)

