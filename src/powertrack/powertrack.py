"""
Wh_m_sum: Modeled AC Energy (kWh sum)
Wh_e_sum: Expected AC Energy (kWh) sum
Ih_e_sum: Expected Insolation (Wh/m²) sum
Snow_m_max: Modeled Snow Depth (mm) max
"""

import re

import math
from datetime import datetime
import time

import numpy as np
import pandas as pd
import json

from utils import *
from src.powertrack.powertrack_api import PowertrackApi
from src.logger.logger import *



class Powertrack(PowertrackApi):
    """Contains all functionality of the Powertrack ETL structure"""
    
    def __init__(self):
        super().__init__()
        self.__desired_short_names = ['Wh_sum', 'Wh_estDowntimeLoss_sum', 'Wh_estClippingLoss_sum',
                            'Wh_estSnowLoss_sum', 'Wh_estDegradLoss_sum', 'Wh_estSoilingLoss_sum',
                            'Wh_estShadingLoss_sum', 'Wh_estOtherLoss_sum', 'Wh_estPartialDowntimeLoss_sum']
        self.__short_names_to_complete_names = { 'Wh_sum': 'measured_energy', 'Wh_estDowntimeLoss_sum': 'downtime',
                           'Wh_estClippingLoss_sum': 'clipping', 'Wh_estSnowLoss_sum': 'snow',
                           'Wh_estDegradLoss_sum': 'degradation', 'Wh_estSoilingLoss_sum': 'soiling',
                           'Wh_estShadingLoss_sum': 'shading', 'Wh_estOtherLoss_sum': 'other',
                           'Wh_estPartialDowntimeLoss_sum': 'partial_downtime' }
        self.__site_level_short_names = { 'Wh_m_sum': 'modeled_energy', 'Wh_e_sum': 'expected_energy', 'Ih_e_sum': 'expected_insolation', 'Snow_m_max': 'max_snow_depth' }
        self.__weatherstation_short_names = { 'POAIh_sum': 'poa_insolation',  'TmpBOM_avg': 'module_temp',  'TmpAmb_avg': 'ambient_temp',  'WndSpd_avg': 'wind_speed' }
        self.__get_component_data_stats = pd.DataFrame(columns=['site_id', 'run_time'])
        self.__site_stats = pd.DataFrame(columns=['site_id', 'run_time'])
        log('Constructor finished for Powertrack')


    def __deconstructor(self):
        """
        Deconstructor - Exporing dfs to csvs
        __del__ does not work well here because it seems that the imports are 
            destroyed before it runs. Making the functions below throw an error
        """
        now = get_time_now()
        super()._deconstructor()
        self.__get_component_data_stats.to_csv(f'csvs/get_component_data_stats_{now}.csv')
        self.__site_stats.to_csv(f'csvs/__site_stats_{now}.csv')
        log('Deconstructor for Powertrack')


    def __get_site_hardware(self, site_id: str) -> tuple[dict, set, set, set]:
        """
        Replacing: pull_component_data
        Getting the component information of a site and keeping the 
            data that is needed. Also classifying the components by 
            their node type

        :param site_id: the monitoring id for the site
        :return component_details: { id: {name: str, nodeType: str, parentId: int (might be str)} }
        :return meter_component_ids: set of ids that are of type METER
        :return inverter_component_ids: set of ids that are of type INVERTER
        """

        try:
            start = time.time()
            #components = PowertrackApi._get_site_components(self, site_id)
            hardware = super()._get_site_hardware(site_id)
            #log(f'powertrack.py,__get_site_hardware: hardware response from super()._get_site_hardware() is {hardware}')
            hardware_details = {}
            meter_ids = set()
            inverter_ids = set()
            weatherstation_ids = set()
            for hw in hardware:  
                hw_id = hw['id']
                hardware_details[hw_id] = {}
                hardware_details[hw_id]['name'] = str(hw['name'])
                hardware_details[hw_id]['functionCode'] = str(hw['functionCode'])
                hardware_details[hw_id]['stringId'] = str(hw['stringId'])
                if hw['functionCode'] == 'PM':
                    meter_ids.add(hw_id)
                elif hw['functionCode'] == 'PV':
                    inverter_ids.add(hw_id)
                elif hw['functionCode'] == 'WS':
                    weatherstation_ids.add(hw_id)

            run_time = time.time() - start
            self.__get_component_data_stats = self.__get_component_data_stats.append({ 'site_id': site_id, 'run_time': run_time }, ignore_index=True)
            log("Right before log(hardware_details)")
            log(hardware_details)
            log(meter_ids)
            log(inverter_ids)
            log(weatherstation_ids)
            return hardware_details, meter_ids, inverter_ids, weatherstation_ids
        
        except Exception as e:
            log('Error in __get_site_hardware')
            log(e)
            return {}, set(), set(), set()


    def __get_daily_averages_of_monthly_proformas(self, asset_id: str, start_date: str, end_date: str, proforma_df) -> pd.DataFrame:
        """
        Replacing: pull_pro_forma
        create daily proforma estimate from monthly proforma estimate

        :param asset_id: asset id (type: str)
        :param start_date: datetime obj (type: obj)
        :param end_date: datetime obj (type: obj)
        :param proforma_df: proforma dataframe from dynamics365
                            for monthly data (type: dataframe)

        :return: proforma dataframe for daily data (type:dataframe)
        """

        start_time = time.time()
        try:
            log(f'asset_id: {asset_id}')
            proforma_df.to_csv('csvs/df1.csv')
            proforma_df = proforma_df.loc[proforma_df['asset_id'] == asset_id]
            timestamps_list = create_timestamp_list(start_date, end_date)
            number_of_timestamps = len(timestamps_list)
            # days_in_months = [val for val in proforma_df.days_in_month]
            proforma_df.to_csv('csvs/df2.csv')

            # When using loc, if the argument is an array that index values then the 
            #   dataframe is reorganized. If the index values are replicated than 
            #   the index row will be replicated. More at the top of a page on this
            proforma_df = proforma_df.loc[np.repeat(proforma_df.index.values, proforma_df.days_in_month)]
            proforma_df.to_csv('csvs/df3.csv')
            proforma_df = proforma_df.reset_index(drop=True)
            proforma_df.to_csv('csvs/df4.csv')
            proforma_df = proforma_df[0:number_of_timestamps]
            proforma_df.index = timestamps_list
            return proforma_df
        except Exception as e:
            log('Error in __get_daily_averages_of_monthly_proformas')
            log(e)
        finally:
            run_time = time.time() - start_time
            log(f'Runtime of __get_daily_averages_of_monthly_proformas: {run_time}')
            return pd.DataFrame()


    def __get_weatherstation_df_by_hardware(self, weatherstation_id, site_id,start_timestamp,end_timestamp):
        """
        Returns a dataframe of the weatherstation data for a single hardware
        
        :param weatherstation_id: The id of the hardware
        :param site_id: the id of the site
        :param start_timestamp: the starting timestamp. Format is yyyy-mm-dd hh:mm:ss 
        :param end_timestamp: the ending timestamp. Format is yyyy-mm-dd hh:mm:ss 
        """
        try:
            poa=[]
            bpoa=[]
            ghi=[]
            windspeed=[]
            temperature=[]
            weatherstation_details = super()._get_site_hardware_details(weatherstation_id)
            weatherstation_name = weatherstation_details["name"]

            for address in weatherstation_details["registerGroups"][0]['registers']:
                # binsize = super()._get_binsize()
                # log(f'binsize is set to {binsize}')
                if "dataName" not in address:
                    continue

                if address["dataName"] == 'Sun' and re.search(r'\bbpoa\b', weatherstation_name, re.IGNORECASE):
                    log(f"{weatherstation_id} is BPOA, address is {address['address']}")
                    bpoa_data = super()._get_data_for_hardware(weatherstation_id, site_id, start_timestamp, end_timestamp, 'Sun','Last')
                    bpoa.extend(bpoa_data['items'])
                    log(f'bpoa data is {bpoa}')
                elif address["dataName"] == 'Sun':
                    log(f"{weatherstation_id} is POA, address is {address['address']}")
                    poa_data = super()._get_data_for_hardware(weatherstation_id, site_id, start_timestamp, end_timestamp, 'Sun','Last')
                    log(f"poa_data from super().get_data_for_hardware poa is {poa_data['items']}")
                    poa.extend(poa_data['items'])
                    log(f'poa data is {poa}')
                elif address["dataName"] == 'Sun2' and re.search(r'\bghi\b', weatherstation_name, re.IGNORECASE):
                    log(f"{weatherstation_id} contains Sun2, address is {address['address']}")
                    ghi_data = super()._get_data_for_hardware(weatherstation_id, site_id, start_timestamp, end_timestamp, 'Sun2','Last')
                    ghi.extend(ghi_data['items'])
                    log(f'ghi data is {ghi}')
                elif address["dataName"] == 'WindSpeed':
                    log(f"{weatherstation_id} contains Windspeed, address is {address['address']}")
                    log(f"Before windspeed_data: {windspeed}")
                    windspeed_data = super()._get_data_for_hardware(weatherstation_id, site_id, start_timestamp, end_timestamp,'WindSpeed','Last')
                    windspeed.extend(windspeed_data['items'])
                    log(f'windspeed_raw_data is {windspeed_data}')
                    log(f'after windspeed_data is {windspeed}')
                elif address["dataName"] == 'TempF':
                    log(f"{weatherstation_id} contains Tempf, address is {address['address']}")
                    temperature_data = super()._get_data_for_hardware(weatherstation_id, site_id, start_timestamp, end_timestamp, "TempF",'Last')
                    log(f"temperature unit is {temperature_data['info'][0]['units']}")
                    if temperature_data['info'][0]['units'] == 'Fahrenheit':
                        celcius_data_list = []
                        for data_entry in temperature_data['items']:
                            timestamp = data_entry['timestamp']
                            fahrenheit = data_entry['data'][0]
                            fahrenheit = float(fahrenheit)
                            celsius = self.__fahrenheit_to_celsius(fahrenheit)
                            celcius_data_list.append({'timestamp': timestamp, 'data': [celsius]})
                        #print(f'celcius list is {celcius_data_list}')
                        temperature.extend(celcius_data_list)
                    else:
                        temperature.extend(temperature_data['items'])
                    log(f'temperature_raw_data is {temperature_data}')
                    log(f'temperature_data is {temperature}')
                else:
                    log(f"{weatherstation_id} don't contain any of these, it contains {weatherstation_details['registerGroups'][0]['registers']}")
                
            dfs = self.__create_weatherstation_dfs_from_hardware_data(poa,bpoa,ghi,windspeed,temperature)
            weatherstation_hardware_df = self.__merge_hardware_weatherstation_dfs(dfs)
            return weatherstation_hardware_df
        
        except Exception as e:
            log(f'error inside __get_weatherstation_df_by_hardware: {e}')
    

    def __fahrenheit_to_celsius(self, fahrenheit):
        """
        Returns celcius calculation of the fahrenheit value
        
        :param fahrenheit: the float in fahrenheit
        """

        if not math.isnan(fahrenheit):
            celsius = (fahrenheit - 32) * (5/9)
            return celsius
        else:
            return float('NaN')
    

    def __merge_hardware_weatherstation_dfs(self, dfs):
        """
        Returns a dataframe of the weatherstation data given a list of dataframes
        
        :param dfs: list of dataframes, each with weatherstation data

        """
                
        non_empty_dfs = [df for df in dfs if not df.empty]
        #log(f'nonempty_df is {non_empty_dfs}')
        if non_empty_dfs:
            weatherstation_hardware_df = non_empty_dfs[0]
            for df in non_empty_dfs[1:]:
                weatherstation_hardware_df = weatherstation_hardware_df.merge(df, on='timestamp', how='outer')
        else:
            print("All dataframes are empty. Skipping merge.")
            weatherstation_hardware_df = None

        if weatherstation_hardware_df is not None:
            #log(f'weahterstation_data before numeric transformation is {weatherstation_hardware_df}')
            for column in weatherstation_hardware_df.columns:
                if weatherstation_hardware_df[column].dtype == 'object':
                    #log(f'column is {column}')
                    weatherstation_hardware_df[column] = weatherstation_hardware_df[column].apply(self.__transform_object_to_numeric)
            #log(f'weatherstation_hardware_df is {weatherstation_hardware_df}')
            return weatherstation_hardware_df


    def __poa_comparisons(self, dataframes):
        """
        Returns a custom poa dataframe based on conditions given a list of dataframes. 
        Currently the condition is if there are multiple poa dataframes and for any given timestamp,
        the difference is greater than 100, we take the max of the poa values, else we take the average 
        of all of the values.
        
        :param dataframes: a list of dataframes, each with different weatherstation data

        """
        try:
            #log(f'inside poas comparisons')
            visited = set()
            poa_df = pd.DataFrame(columns = ['timestamp', 'poa'])
            test = pd.DataFrame(columns = ['timestamp', 'poa'])
            for i, df in enumerate(dataframes):
                if 'poa' not in df.columns:
                    continue
                for index, row in df.iterrows():
                    poa = row['poa']
                    ts = row['timestamp']
                    if ts in visited: 
                        continue
                    visited.add(ts)
                    poas = [poa]
                    for j, df2 in enumerate(dataframes):
                        if not re.search(r'\bpoa\b', ' '.join(df2.columns), re.IGNORECASE):
                            #log(f'skipping df because columns do not contain poa, columns are {df2.columns}')
                            continue
                        if i ==j:
                            continue
                        row2 = df2.loc[df2['timestamp'] == ts]
                        if not row2.empty:
                            poas.append(row2['poa'].squeeze())    
                    #log(f'poas are {poas}')
                    max_difference = abs(max(poas) - min(poas))
                    if max_difference > 100:
                        poa_final = max(poas)
                    else:
                        poa_average = sum(poas) / len(poas)
                        poa_final = poa_average
                    #poa_df.loc[len(poa_df)] = {'timestamp': ts, 'poa': poa_final}
                    #poa_df = poa_df.append({'timestamp': ts, 'poa': poa_final}, ignore_index=True)
                    row_data = pd.DataFrame([{'timestamp': ts, 'poa': poa_final}])
                    poa_df = pd.concat([poa_df, row_data], ignore_index=True)
            #log(f'poa_df is {poa_df}')
            return poa_df
        
        except Exception as e:
            log(f'exception from __poa_comparisons is {e}')


    def __create_weatherstation_dfs_from_hardware_data(self,poa,bpoa,ghi,windspeed,temperature):
        """
        Returns a weatherstation dataframe of from the list of given data in json format
        
        :param poa: poa data formatted in a list of dictionaries.
        :param bpoa: bpoa data formatted in a list of dictionaries.
        :param ghi: ghi data formatted in a list of dictionaries.
        :param windspeed: windspeed data formatted in a list of dictionaries.
        :param temperature: temperature data formatted in a list of dictionaries.
        """
        poa_df = pd.DataFrame(poa)
        poa_df = poa_df.rename(columns ={'data': 'poa'})
        bpoa_df = pd.DataFrame(bpoa)
        bpoa_df = bpoa_df.rename(columns ={'data': 'bpoa'})
        ghi_df = pd.DataFrame(ghi)
        ghi_df = ghi_df.rename(columns ={'data': 'ghi'})
        windspeed_df = pd.DataFrame(windspeed)
        windspeed_df = windspeed_df.rename(columns = {'data': 'windspeed (MPH)'})
        temperature_df = pd.DataFrame(temperature)
        temperature_df = temperature_df.rename(columns = {'data': 'temperature (celcius)'})
        dfs = [poa_df, bpoa_df, ghi_df, windspeed_df, temperature_df]
        log("RIGHT BEFORE PRINTING WEATHERSTATION LISTS")
        log(windspeed)
        log(temperature)
        log("RIGHT BEFORE PRINTING WEATHERSTATION DFS")
        for df in dfs:
            log(df)
        return dfs
    

    def __transform_object_to_numeric(self,x):
        """
        Returns the data inside a list x. If it is not a list, return the x
        
        :param x: The data

        """

        if isinstance(x, list) and len(x) == 1:
            if x[0] == 'NaN':
                return np.nan
            else:
                return x[0]
        return x


    def __get_weatherstation_df_by_site(self, weatherstation_ids, site_id, start_timestamp, end_timestamp):
        """
        Returns a weatherstation dataframe of of the site

        :param weatherstation_ids: the id of the weatherstations for a given site
        :param site_id: the id of the site
        :param start_timestamp: the starting timestamp. Format is yyyy-mm-dd hh:mm:ss 
        :param end_timestamp: the ending timestamp. Format is yyyy-mm-dd hh:mm:ss 
        """
        try:
            dataframes = []
            for weatherstation in weatherstation_ids:
                dataframe = self.__get_weatherstation_df_by_hardware(weatherstation, site_id, start_timestamp, end_timestamp)
                if dataframe is not None:
                    dataframes.append(dataframe)

            if not dataframes:
                return pd.DataFrame()

            poa_df = self.__poa_comparisons(dataframes)
            weatherstation_site_df = pd.concat(dataframes, axis=0)
            weatherstation_site_df = weatherstation_site_df.groupby('timestamp', as_index=False, sort=False).mean()
            # Drop all POA columns
            weatherstation_site_df = weatherstation_site_df.drop('poa', axis=1)
            # Then merge/concat merged_df with poa_df
            weatherstation_site_df = weatherstation_site_df = pd.merge(weatherstation_site_df, poa_df, on='timestamp', how='outer')
            #log(f'get_weatherstation_df_by_site: merged_df = {weatherstation_site_df}')
            return weatherstation_site_df
        except Exception as e:
            log(f'exception from __get_weatherstation_df_by_site is {e}')


    def __get_meter_df_site(self, meter_ids, site_id, start_timestamp, end_timestamp ):
        """
        Returns meter dataframe for a given site

        :param meter_ids: the ids of the meters
        :param site_id: the id of the site
        :param start_timestamp: the starting timestamp. Format is yyyy-mm-dd hh:mm:ss 
        :param end_timestamp: the ending timestamp. Format is yyyy-mm-dd hh:mm:ss 

        """
        meter_df = pd.DataFrame()
        for meter in meter_ids:
            meter_details = super()._get_site_hardware_details(meter)
            log(f'meter_detailsa are {meter_details}')
            meter_production_data = super()._get_data_for_hardware(meter, site_id, start_timestamp, end_timestamp, 'KW','Last')
            log(f'meter_production_data is {meter_production_data}')
            temp_df = pd.json_normalize(meter_production_data, record_path = ['items'])
            temp_df['meter_id'] = meter
            temp_df = temp_df.rename(columns ={'data': 'production'})
            meter_df = pd.concat([meter_df,temp_df])

        meter_df['production'] = meter_df['production'].apply(lambda x: x[0] if isinstance(x, list) and len(x) == 1 else x)
        meter_df['production'] = pd.to_numeric(meter_df['production'], errors='coerce')
        sum_of_meter_production = meter_df.groupby('timestamp')['production'].sum()
        sum_dict = sum_of_meter_production.to_dict()
        meter_df['production_sum'] = meter_df['timestamp'].map(sum_dict)
        meter_df = meter_df.sort_values(by ='timestamp')
        #log(f'meter_df is {meter_df}')
        return meter_df


    def testme(self):
        # PowertrackApi._testme(self)
        super()._testme()
        print("Running Powertrack Test...")

        try:
            cd, md, ic = self.__get_site_components('3678856')
            print(cd)
            print(md)
            print(ic)
        except Exception as e:
            log(f'exception is {e}')


    def mock_main(self, proforma_df):
        """Used as a mock main, similar to testme but should be more concrete here"""
        mock_main_start_time = time.time()
        now = get_time_now()
        # "Wh_sum", 
        count = 0
        proforma_df = proforma_df[proforma_df['monitoring_platform'] == 'PowerTrack'].astype({"site_id": np.int64})
        unique_proforma = proforma_df.drop_duplicates('asset_id', keep='first')
        all_basefields = []
        site_data_df = pd.DataFrame()
        proforma_df = { 'site_id': ['38054'] }
        site_list = [58481]
        #for site_id in proforma_df['site_id']:
        for index, row in unique_proforma.iterrows():
            try:
                site_start_time = time.time()
                log('================================================================================================================================')
                site_id = row['site_id']
                if site_id not in site_list:
                    continue
                log(f'site_id: {site_id}')
                site_details = super()._get_site_details(site_id)
                log(f'site_details is {site_details}')
                if "error" in site_details:
                    log(f'cannot get site_details for {site_id}, skipping site')
                    continue
                site_name = site_details["name"]
                super()._set_start_timestamp(reformat_date_to_timestamp(row['start_date']))
                super()._set_end_timestamp(get_todays_timestamp()) 
                hardware_details, meter_ids, inverter_ids, weatherstation_ids = self.__get_site_hardware(site_id)
                log(f'meter_ids are {meter_ids}')
                #super()._set_binsize(next(iter(meter_ids)), site_id, '5min')
                super()._set_binsize(next(iter(meter_ids)), site_id, '15min','2022-10-11T00:00:00', '2022-10-22T00:00:00')
                log(super()._get_start_timestamp())
                log(f'parent timeframe is {super()._get_start_timestamp(), super()._get_end_timestamp()}')
                weatherstation_site_df = pd.DataFrame()
                weatherstation_site_df = self.__get_weatherstation_df_by_site( weatherstation_ids, site_id, super()._get_start_timestamp(), super()._get_end_timestamp())
                meter_site_df =  self.__get_meter_df_site(meter_ids, site_id, super()._get_start_timestamp(), super()._get_end_timestamp())
                if weatherstation_site_df is None:
                    continue
                meter_and_weatherstation_df = meter_site_df.merge(weatherstation_site_df, on='timestamp', how='outer')
                meter_and_weatherstation_df['site_id'] = site_id
                meter_and_weatherstation_df['name'] = site_name
                meter_and_weatherstation_df = meter_and_weatherstation_df.sort_values(by=['timestamp'])
                meter_and_weatherstation_df = meter_and_weatherstation_df.round(2)
                #meter_and_weatherstation_df.to_csv(f'csvs/df_for_site_{site_id}-{now}.csv', index=False)
                site_data_df = pd.concat([site_data_df,meter_and_weatherstation_df])

                #formatted_data = data[]
                # component_details, meter_ids, inverter_ids = self.__get_site_components(site_id)
                # intervaled_timestamps, all_timestamps = get_timestamps(start_timestamp)
                # meter_name_to_production_data = self.__get_meter_data(component_details, meter_ids, intervaled_timestamps)
                # aggregated_component_df = self.__sum_component_data(meter_name_to_production_data)

                # # TODO: Find out what to do with unique profroma and how to handle the two proforma dataframes!
                # proforma_information = self.__get_daily_averages_of_monthly_proformas(row['asset_id'], start_timestamp, end_timestamp, proforma_df)
                
                # short_names = np.fromiter(self.__weatherstation_short_names.keys(), dtype=np.dtype('U32'))
                # site_poa_insolation_data_df = self.__get_weatherstation_data(component_details, intervaled_timestamps, short_names)
                # now = get_time_now()
                # site_poa_insolation_data_df.to_csv(f'csvs/site_poa_insolation_data_df_{site_id}_{now}.csv')

                # # get site data
                # expected_site_data = self.__get_expected_site_data(site_id, intervaled_timestamps)

                # self.__get_alerts(site_id, intervaled_timestamps)
                count += 1
                site_run_time = time.time() - site_start_time
                log(f'Site {site_id} ran for {site_run_time} seconds')
                self.__site_stats = self.__site_stats.append({ 'site_id': site_id, 'run_time': site_run_time }, ignore_index=True)
                super()._reset_binsize()
            except Exception as e:
                log(e)
            finally:
                count += 1
                # if count > 15:
                #     break
        log(f'site_data_df is {site_data_df}')
        site_data_df = site_data_df.sort_values(by=['site_id','timestamp'])
        column_order = ['timestamp', 'site_id', 'production', 'meter_id', 'production_sum','poa', 'bpoa', 'ghi', 'windspeed', 'temperature (celcius)','name']
        site_data_df = site_data_df.reindex(columns=column_order)
        site_data_df.to_csv(f'csvs/site_data_df{now}.csv', index=False)
        mock_main_run_time = time.time() - mock_main_start_time
        log(f'Runtime of mock_main: {mock_main_run_time} ({mock_main_run_time / 60} minutes)')
        self.__deconstructor()

    def main(self, site, start_timestamp, end_timestamp, timeframe) -> pd.DataFrame :
        """Used as a mock main, similar to testme but should be more concrete here"""
        main_start_time = time.time()
        now = get_time_now()
        site_data_df = pd.DataFrame()
        try:
            site_start_time = time.time()
            log('================================================================================================================================')
            site_id = site
            log(f'site_id: {site_id}')
            site_details = super()._get_site_details(site_id)
            log(f'site_details is {site_details}')
            if "error" in site_details:
                log(f'cannot get site_details for {site_id}, skipping site')
                return pd.DataFrame()
            site_name = site_details["name"]
            super()._set_start_timestamp(start_timestamp)
            super()._set_end_timestamp(get_todays_timestamp()) 
            hardware_details, meter_ids, inverter_ids, weatherstation_ids = self.__get_site_hardware(site_id)
            log(f'meter_ids are {meter_ids}')
            #super()._set_binsize(next(iter(meter_ids)), site_id, '5min')
            super()._set_binsize(next(iter(meter_ids)), site_id, timeframe, start_timestamp, end_timestamp)
            log(super()._get_start_timestamp())
            log(f'parent timeframe is {super()._get_start_timestamp(), super()._get_end_timestamp()}')
            weatherstation_site_df = pd.DataFrame()
            weatherstation_site_df = self.__get_weatherstation_df_by_site( weatherstation_ids, site_id, super()._get_start_timestamp(), super()._get_end_timestamp())
            meter_site_df =  self.__get_meter_df_site(meter_ids, site_id, super()._get_start_timestamp(), super()._get_end_timestamp())
            if weatherstation_site_df is None:
                return meter_site_df
            site_data_df = meter_site_df.merge(weatherstation_site_df, on='timestamp', how='outer')
            site_data_df['site_id'] = site_id
            site_data_df['name'] = site_name
            site_data_df = site_data_df.sort_values(by=['timestamp'])
            site_data_df = site_data_df.round(2)
            log(f'site_data_df is {site_data_df}')
            site_data_df = site_data_df.sort_values(by=['site_id','timestamp'])
            column_order = ['timestamp', 'site_id', 'production', 'meter_id', 'production_sum','poa', 'bpoa', 'ghi', 'windspeed (MPH)', 'temperature (celcius)','name']
            site_data_df = site_data_df.reindex(columns=column_order)
            site_data_df.to_csv(f'csvs/site_data_df_{now}_binsize={timeframe}.csv', index=False)
            main_start_time = time.time() - main_start_time
            log(f'Runtime of mock_main: {main_start_time} ({main_start_time / 60} minutes)')
            self.__deconstructor()
            return site_data_df
            #meter_and_weatherstation_df.to_csv(f'csvs/df_for_site_{site_id}-{now}.csv', index=False)

            #formatted_data = data[]
            # component_details, meter_ids, inverter_ids = self.__get_site_components(site_id)
            # intervaled_timestamps, all_timestamps = get_timestamps(start_timestamp)
            # meter_name_to_production_data = self.__get_meter_data(component_details, meter_ids, intervaled_timestamps)
            # aggregated_component_df = self.__sum_component_data(meter_name_to_production_data)

            # # TODO: Find out what to do with unique profroma and how to handle the two proforma dataframes!
            # proforma_information = self.__get_daily_averages_of_monthly_proformas(row['asset_id'], start_timestamp, end_timestamp, proforma_df)
            
            # short_names = np.fromiter(self.__weatherstation_short_names.keys(), dtype=np.dtype('U32'))
            # site_poa_insolation_data_df = self.__get_weatherstation_data(component_details, intervaled_timestamps, short_names)
            # now = get_time_now()
            # site_poa_insolation_data_df.to_csv(f'csvs/site_poa_insolation_data_df_{site_id}_{now}.csv')

            # # get site data
            # expected_site_data = self.__get_expected_site_data(site_id, intervaled_timestamps)

            # self.__get_alerts(site_id, intervaled_timestamps)
            site_run_time = time.time() - site_start_time
            log(f'Site {site_id} ran for {site_run_time} seconds')
            self.__site_stats = self.__site_stats.append({ 'site_id': site_id, 'run_time': site_run_time }, ignore_index=True)
            super()._reset_binsize()
        except Exception as e:
            log(e)

   