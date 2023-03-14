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
            # components = PowertrackApi._get_site_components(self, site_id)
            hardware = super()._get_site_hardware(site_id)
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
            return hardware_details, meter_ids, inverter_ids, weatherstation_ids
        except Exception as e:
            log('Error in __get_site_hardware')
            log(e)
            return {}, set(), set(), set()


    def __get_meter_data(self, component_details: dict, meter_ids: set, intervaled_timestamps: list[list[str]]) -> dict[str, pd.DataFrame]:
        """
        Replacing: find_relevant_data
        Iterating though all meters of a site and extracting short_name data from it

        :param component_details: dictionary containing details of a component 
                                { component_id: { name: }, { nodeType: }, { parentId: } }
        :param meter_ids: set of meter_ids
        :param intervaled_timestamps: [[timestamps], [timestamps]] each inner list is 
                                up to 1500 elements
        :return: dictionary that is contains datapoints of components
                { component_id: pd.DataFrame }
                Where the dataframe contains all the datapoints
        """

        try:
            meter_names_to_data = {}   
            for meter_id in meter_ids:
                log(f'Getting meter data for {meter_id}')
                available_meter_data = super()._get_data_available_for_component(meter_id)
                short_names = self.__extract_short_names(available_meter_data)
                joined_short_names = ','.join(short_names)
                meter_data = super()._get_data_for_component(meter_id, intervaled_timestamps, joined_short_names)
                meter_data_df = self.__extract_short_name_data(meter_data, short_names)
                meter_name = component_details[meter_id]['name']
                meter_names_to_data[meter_name] = meter_data_df
            log(f'Got all meter data for the site')
            return meter_names_to_data
        except Exception as e:
            log('Error in __get_meter_data')
            log(e)


    def __extract_short_name_data(self, data: list[dict], short_names: np.ndarray) -> pd.DataFrame:
        """
        TODO: ADD FEATURE TO USE SITE LEVEL SHORT NAMES
        Replacing: pull_expected_data (I think)
        Iterate through the component_data and get the short_name information
            from it. Place it into arrays and then create a dataframe of it.
        
        :param component_data: response from Powertrack containing data from a desired component
        :param short_name: list of short_names
        :return: dataframe containing all short_name data where the index is the timestamps
        """

        start_time = time.time()
        try:
            # timestamps, datapoints = np.empty(0), np.empty(0)
            all_data_dfs = []
            short_names = np.append(short_names, 'ts')
            datapoints = { short_name: np.empty(0) for short_name in short_names }
            log(datapoints)
            for datapoint in data:
                for short_name in short_names:
                    try:
                        datapoints[short_name] = np.append(datapoints[short_name], datapoint.get(short_name))
                    except KeyError:
                        datapoints[short_name] = np.append(datapoints[short_name], None)

            data_df = pd.DataFrame(datapoints)
            data_df.set_index('ts', inplace=True, drop=True, append=False)                
            data_df = data_df / 1000 
            log(data_df)
            # all_data_dfs.append(data_df)

            return data_df #pd.concat(all_data_dfs, axis=1, sort=False)
        except Exception as e:
            log('Error in __extract_short_name_data')
            log(e)
        finally:
            run_time = time.time() - start_time
            log(f'Runtime of __extract_short_name_data: {run_time}')


    def __extract_short_names(self, available_component_data: list[dict]) -> np.ndarray:
        """
        Iterating through the available_component_data list and extracting the desired
            short_names from it. 

        :param available_component_data: response from Powertrack containing all basefields
            of a specific component. Example of the response in jsons folder
        :return: list of shorts_names
        """

        try:
            short_names = np.empty(0)
            for basefield in available_component_data:
                for aggregation in basefield['aggregations']:
                    if aggregation.get('shortName') in self.__desired_short_names:
                        short_names = np.append(short_names, aggregation.get('shortName'))
            return short_names
        except Exception as e:
            log('Error in __extract_short_names')
            log(e)


    def __sum_component_data(self, component_name_to_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Replacing: aggregate_data
        Summing up the component data of a site into one dataframe.
            This will give us the site value for each day that is included

        :param component_name_to_data: component data
                                    { component_id: pd.DataFrame }
        :return: summed up component data. 
                Essentially, the sitelevel data where each row is a day
        """

        try:
            component_data_dfs = list(component_name_to_data.values())
            site_level_data = component_data_dfs[0]

            for i in range(1, len(component_data_dfs)):
                component_data_dfs = component_data_dfs[i]
                site_level_data = site_level_data.add(component_data_dfs, fill_value=0)

            return site_level_data
        except Exception as e:
            log('Error in __sum_component_data')
            log(e)


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


    def __get_components_by_type(self, component_details: dict[str, dict[str, str]], component_type: str) -> list[str]:
        """
        Return a list of component details based on a desired component type
            i.e. if component_type=='WEATHERSTATION' 
                then return all component details where the nodeType == WEATHERSTATION
        
        :param component_details: { id: {name: str, nodeType: str, parentId: str} }
        :param component_type: the desired component type
        :return: list of component ids that match the desired component_type
        """

        try:
            desired_component_ids = []
            for component in component_details:
                if component_details[component]['nodeType'] == component_type:
                    desired_component_ids.append(component)
            return desired_component_ids
        except Exception as e:
            log('Error in __get_components_by_type')
            log(e)


    def __get_weatherstation_data(self, component_details: dict[str, dict[str, str]], intervaled_timestamps: list[list[str]], short_names: np.ndarray) -> pd.DataFrame:
        """
        Replacing: pull_poa_insolation & pull_module_data
        Getting all the poa data

        :params component_details: dictionary of component details
        :param intervaled_timestamps: timestamps
        :param short_name_details: dictionary containing the shortname and what the shortname should be
            called in the dataframe
                    i.e. { 'name' poa_insolation, 'short_name': POA_INS } TODO: This is now an array
        :return: pd.DataFrame that contains the data from the weatherstation and the index is the timestamps
        """

        start_time = time.time()
        try:
            weatherstations = self.__get_components_by_type(component_details, 'WEATHERSTATION')
            weatherstation_df_max_length = -1
            weatherstation_df = pd.DataFrame()
            for weatherstation in weatherstations:
                weatherstation_data = super()._get_data_for_component(weatherstation, intervaled_timestamps, ','.join(map(str, short_names)))
                weatherstation_df_temp = self.__extract_short_name_data(weatherstation_data, short_names)                

                if len(weatherstation_df_temp) > weatherstation_df_max_length:
                    weatherstation_df = weatherstation_df_temp
            
            return weatherstation_df
        except Exception as e:
            log('Error in __get_weatherstation_data')
            log(e)
        finally:
            run_time = time.time() - start_time
            log(f'Runtime of __get_weatherstation_data: {run_time}')
  
    
    def __get_expected_site_data(self, site_id: str, intervaled_timestamps: list[list[str]]) -> pd.DataFrame:
        """
        """

        try:
            short_names = np.fromiter(self.__site_level_short_names.keys(), dtype=np.dtype('U32'))
            short_names_str = ','.join(short_names)
            site_data = super()._get_data_for_site(site_id, intervaled_timestamps, short_names_str)
            site_data_df = self.__extract_short_name_data(site_data, short_names)
            return site_data_df
        except Exception as e:
            log(f'Error in __get_expected_site_data | {site_id}')
            log(e)


    def __get_alerts(self, site_id: str, intervaled_timestamps: list[list[str]]) -> pd.DataFrame:
        """
        """
        alerts = super()._get_site_alerts(site_id)
        return pd.DataFrame()


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
            log(e)


    def mock_main(self, proforma_df):
        """Used as a mock main, similar to testme but should be more concrete here"""

        mock_main_start_time = time.time()

        # "Wh_sum", 
        count = 0
        proforma_df = proforma_df[proforma_df['monitoring_platform'] == 'PowerTrack'].astype({"site_id": np.int64})
        unique_proforma = proforma_df.drop_duplicates('asset_id', keep='first')
        all_basefields = []
        site_data_df = pd.DataFrame()
        # proforma_df = { 'site_id': ['3892217'] }
        # for site_id in proforma_df['site_id']:
        for index, row in unique_proforma.iterrows():
            try:
                site_start_time = time.time()
                log('================================================================================================================================')
                site_id = row['site_id']
                log(f'site_id: {site_id}')
                start_timestamp = reformat_date_to_timestamp(row['start_date'])
                end_timestamp = get_todays_timestamp()

                hardware_details, meter_ids, inverter_ids, weatherstation_ids = self.__get_site_hardware(site_id)
                log(meter_ids)
                for meter in meter_ids:
                    super()._get_data_for_hardware(meter, site_id, start_timestamp, end_timestamp, 'KWHDel')

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

            except Exception as e:
                log(e)
            finally:
                count += 1
                if count > 2:
                    break
    
        mock_main_run_time = time.time() - mock_main_start_time
        log(f'Runtime of mock_main: {mock_main_run_time} ({mock_main_run_time / 60} minutes)')
        self.__deconstructor()
