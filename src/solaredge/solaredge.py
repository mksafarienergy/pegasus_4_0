"""
Wh_m_sum: Modeled AC Energy (kWh sum)
Wh_e_sum: Expected AC Energy (kWh) sum
Ih_e_sum: Expected Insolation (Wh/m²) sum
Snow_m_max: Modeled Snow Depth (mm) max
"""

from datetime import datetime
import time

import numpy as np
import pandas as pd

from solaredge_api import SolarEdgeApi



class SolarEdge(SolarEdgeApi):
    """Contains all functionality of the SolarEdge structure"""
    
    def __init__(self):
        super().__init__()

    def __get_energy_for_site_in_dataframe(self, site_id, start_date, end_date, time_unit):
        data = super()._get_energy_for_site(site_id,start_date,end_date,time_unit)
        df = pd.json_normalize(data, meta='siteId', record_path=['energyValues','values'])
        return df
    
    def __get_site_details_in_dataframe(self, site_id):
        data = super()._get_site_details(site_id)
        df = pd.json_normalize(data)
        return df
    
    def __get_site_components_in_dataframe(self, site_id):
        data= super()._get_site_components(site_id)
        df = pd.json_normalize(data)
        return df
    
    def __get_inveter_data_in_dataframe(self, site_id, serial_number, start_timestamp, end_timestamp):
        data = super()._get_inverter_data(site_id, serial_number, start_timestamp, end_timestamp)
        df = pd.json_normalize(data)
        df['serialNumber'] = serial_number
        return df
    
    def __get_sensor_data_in_dataframe(self, site_id, start_timestamp, end_timestamp):
        data = super()._get_sensor_data(site_id, start_timestamp, end_timestamp)
        df = pd.json_normalize(data)
        return df 
    
    def mock_main(self):
        id=2256292
        details= self.__get_site_details_in_dataframe(id)
        components = self.__get_site_components_in_dataframe(id)
        inverter_data = self.__get_inveter_data_in_dataframe(id,'7E09D12D-85', '2023-03-13 11:00:00', '2023-03-13 13:00:00')
        sensor_data = self.__get_sensor_data_in_dataframe(id,'2023-03-13 00:00:00','2023-03-13 23:59:59')
        energy = self.__get_energy_for_site_in_dataframe(id,'2023-03-12','2023-03-14','DAY')
        merged_df = pd.merge(energy, details, left_on='siteId', right_on='id').drop('id', axis=1)
        print("END")
        #print(solar_edge._get_sensor_data(2184009,'2023-03-13 00:00:00','2023-03-13 23:59:59'))


solar_edge=SolarEdge()


solar_edge.mock_main()

