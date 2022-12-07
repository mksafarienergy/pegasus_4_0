from datetime import datetime
import time

import numpy as np
import pandas as pd

from utils import *
from src.locus.locus_api import LocusApi
from src.logger.logger import *



class Locus(LocusApi):
    """Contains all functionality of the Locus ETL structure"""
    
    def __init__(self):
        super().__init__()
        self.__desired_short_names = ['Wh_sum', 'Wh_estDowntimeLoss_sum', 'Wh_estClippingLoss_sum',
                            'Wh_estSnowLoss_sum', 'Wh_estDegradLoss_sum', 'Wh_estSoilingLoss_sum',
                            'Wh_estShadingLoss_sum', 'Wh_estOtherLoss_sum', 'Wh_estPartialDowntimeLoss_sum']
        self.__short_names_to_readable_names = { 'Wh_sum': 'measured_energy', 'Wh_estDowntimeLoss_sum': 'downtime',
                           'Wh_estClippingLoss_sum': 'clipping', 'Wh_estSnowLoss_sum': 'snow',
                           'Wh_estDegradLoss_sum': 'degradation', 'Wh_estSoilingLoss_sum': 'soiling',
                           'Wh_estShadingLoss_sum': 'shading', 'Wh_estOtherLoss_sum': 'other',
                           'Wh_estPartialDowntimeLoss_sum': 'partial_downtime' }
        self.__get_component_data_stats = pd.DataFrame(columns=['site_id', 'run_time'])
        log('Constructor finished for Locus')


    def __deconstructor(self):
        """
        Deconstructor - Exporing dfs to csvs
        __del__ does not work well here because it seems that the imports are 
            destroyed before it runs. Making the functions below throw an error
        """

        now = get_time_now()
        super()._deconstructor()
        self.__get_component_data_stats.to_csv(f'csvs/get_component_data_stats_{now}.csv')
        log('Deconstructor for Locus')

    
    def __get_site_components(self, site_id: str) -> tuple[dict, set, set]:
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

        start = time.time()
        # components = LocusApi._get_site_components(self, site_id)
        components = super()._get_site_components(site_id)
        component_details = {}
        meter_ids = set()
        inverter_ids = set()

        for component in components:
            component_id = component['id']

            component_details[component_id] = {}
            component_details[component_id]['name'] = str(component['name'])
            component_details[component_id]['nodeType'] = str(component['nodeType'])
            component_details[component_id]['parentId'] = str(component['parentId'])

            if component['nodeType'] == 'METER':
                meter_ids.add(component_id)
            elif component['nodeType'] == 'INVERTER':
                inverter_ids.add(component_id)

        run_time = time.time() - start
        self.__get_component_data_stats = self.__get_component_data_stats.append({ 'site_id': site_id, 'run_time': run_time }, ignore_index=True)
        return component_details, meter_ids, inverter_ids


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
        """

        meter_ids_to_data = {}   
        # count = 1     
        for meter_id in meter_ids:
            log(f'Getting meter data for {meter_id}')
            available_meter_data = super()._get_data_available_for_component(meter_id)
            short_names = self.__extract_short_long_names(available_meter_data)
            joined_short_names = ','.join(short_names)
            meter_data = super()._get_data_for_component(meter_id, intervaled_timestamps, joined_short_names)
            meter_data_df = self.__extract_component_short_name_data(meter_data, short_names)
            meter_name = component_details[meter_id]['name']
            meter_ids_to_data [meter_name] = meter_data_df
            # meter_data_df.to_csv(f'csvs/3621460_{count}.csv')
            # count += 1
        log(f'Got all meter data for the site')
        return meter_ids_to_data


    def __extract_component_short_name_data(self, component_data: list[dict], short_names: np.ndarray) -> pd.DataFrame:
        """
        Iterate through the component_data and get the short_name information
            from it. Place it into arrays and then create a dataframe of it.
        
        :param component_data: response from Locus containing data from a desired component
        :param short_name: list of short_names
        :return: dataframe containing all short_name data where the index is the timestamps
        """

        timestamps, datapoints = np.empty(0), np.empty(0)
        all_data_dfs = []
        for short_name in short_names:
            for component_datapoint in component_data:
                if component_datapoint.get(short_name):
                    timestamps = np.append(timestamps, component_datapoint['ts'])
                    datapoints = np.append(datapoints, component_datapoint.get(short_name))
        
            # Converting datapoints from wh to kwh
            datapoints_in_kwh = datapoints / 1000
            datapoints_dict = { self.__short_names_to_readable_names[short_name]: datapoints_in_kwh, 'timestamps': timestamps }
            data_df = pd.DataFrame(datapoints_dict)
            data_df.set_index('timestamps', inplace=True, drop=True, append=False)
            all_data_dfs.append(data_df)

        return pd.concat(all_data_dfs, axis=1, sort=False)
        


    def __extract_short_long_names(self, available_component_data: list[dict]) -> np.ndarray:
        """
        Iterating through the available_component_data list and extracting the desired
            short_names from it. 

        :param available_component_data: response from Locus containing all basefields
            of a specific component. Example of the response in jsons folder
        :return: list of shorts_names
        """

        short_names = np.empty(0)
        for basefield in available_component_data:
            for aggregation in basefield['aggregations']:
                if aggregation.get('shortName') in self.__desired_short_names:
                    short_names = np.append(short_names, aggregation.get('shortName'))
        return short_names


    def __sum_component_data(self, component_id_to_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Replacing: aggregate_data
        Summing up the component data of a site into one dataframe.
            This will give us the site value for each day that is included

        :param component_id_to_data: component data
                                    { component_id: pd.DataFrame }
        :return: summed up component data. 
                Essentially, the sitelevel data where each row is a day
        """

        component_data_dfs = list(component_id_to_data.values())
        site_level_data = component_data_dfs[0]

        for i in range(1, len(component_data_dfs)):
            component_data_dfs = component_data_dfs[i]
            site_level_data = site_level_data.add(component_data_dfs, fill_value=0)

        return site_level_data


    def __pull_pro_forma(self, asset_id, start_date, end_date, proforma_df):
        """
        create daily estimate from monthly estimate using proforma data

        :param asset_id: asset id (type: str)
        :param start_date: datetime obj (type: obj)
        :param end_date: datetime obj (type: obj)
        :param proforma_df: proforma dataframe from dynamics365
                            for monthly data (type: dataframe)

        :return: proforma dataframe for daily data (type:dataframe)
        """

        try:
            df = proforma_df.loc[proforma_df['asset_id'] == asset_id]
            log('__pull_pro_forma1')
            timestamps_list = create_timestamp_list(start_date, end_date)
            log('__pull_pro_forma2')
            length_of_df = len(timestamps_list)
            log('__pull_pro_forma3')
            replications = [val for val in df.days_in_month]
            log('__pull_pro_forma4')
            df = df.loc[np.repeat(df.index.values, replications)]
            log('__pull_pro_forma5')
            df = df.reset_index(drop=True)
            log('__pull_pro_forma6')
            df.to_csv('csvs/df1.csv')
            log('__pull_pro_forma7')
            df_new = df[0:length_of_df]
            log('__pull_pro_forma8')
            df.to_csv('csvs/df2.csv')
            log('__pull_pro_forma9')
            df_new.index = timestamps_list
            log("Hello5")
            return df_new
        except Exception as e:
            log('e')
            log(e)


    def testme(self):
        # LocusApi._testme(self)
        super()._testme()
        print("Running Locus Test...")

        try:
            cd, md, ic = self.__get_site_components('3678856')
            print(cd)
            print(md)
            print(ic)
        except Exception as e:
            log(e)


    def mock_main(self, proforma_df):
        """Used as a mock main, similar to testme but should be more concrete here"""

        start_time = time.time()

        # "Wh_sum", 
        count = 0
        proforma_df = proforma_df[proforma_df['monitoring_platform'] == 'Locus'].astype({"site_id": int})
        all_basefields = []
        # proforma_df = { 'site_id': ['3621460'] }
        # for site_id in proforma_df['site_id']:
        for index, row in proforma_df.iterrows():
            try:
                site_id = row['site_id']
                
                start_date = reformat_date_to_timestamp(row['start_date'])
                end_date = get_todays_timestamp()

                component_details, meter_ids, inverter_ids = self.__get_site_components(site_id)
                intervaled_timestamps, all_timestamps = get_timestamps(start_date)
                meter_data = self.__get_meter_data(component_details, meter_ids, intervaled_timestamps)
                site_level_data = self.__sum_component_data(meter_data)

                proforma_information = self.__pull_pro_forma(row['asset_id'], start_date, end_date, proforma_df)

                # final_df.to_csv('csvs/final_df.csv')
                count += 1
            except Exception as e:
                log(e)
                count += 1
            if count > 5:
                break
    
        run_time = time.time() - start_time
        log(f'Runtime of mock_main: {run_time}')
        self.__deconstructor()
