import json
import time
import warnings
from math import floor
from dateutil import rrule
from datetime import datetime

import pandas as pd

from src.dynamics.dynamics_auth import create_session, use_dynamics_session
# from utils import get_time_now

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

API_URL = 'https://safarienergy.api.crm.dynamics.com/api/data/v9.2/'

month_energy_list = ['jan_energy', 'feb_energy', 'mar_energy', 'apr_energy', 'may_energy', 'jun_energy',
                     'jul_energy', 'aug_energy', 'sep_energy', 'oct_energy', 'nov_energy', 'dec_energy']
month_insolation_list = ['jan_inso', 'feb_inso', 'mar_inso', 'apr_inso', 'may_inso', 'jun_inso',
                         'jul_inso', 'aug_inso', 'sep_inso', 'oct_inso', 'nov_inso', 'dec_inso']
month_name_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def get_assets(session) -> pd.DataFrame:
    """
    Getting data from the asset table in Dynamics. Cleaning up the data slightly to 
        remove uneccessary information and formatting it.

    :param session: session for the connection to the Dynamics API
    :return: DataFrame with asset information and another DataFrame with same data
    """

    res = use_dynamics_session(session, API_URL+'msdyn_customerassets?$select=msdyn_name,cre90_projectnumber,cre90_monitoringplatform,\
                                   cre90_monitoringplatformid,msdyn_customerassetid,cre90_availableinpegasus,cre90_opportunityaddress,cre90_timezone')
    res_json = res.content.decode('utf-8')
    asset_data = json.loads(res_json)
    df_assets = pd.DataFrame(asset_data['value']).drop(['@odata.etag'], axis=1)
    df_assets = df_assets[df_assets['cre90_availableinpegasus'] == True]
    df_assets = df_assets.rename(columns={'msdyn_name': 'asset_name',
                                          'cre90_projectnumber': 'project_id',
                                          'cre90_monitoringplatform': 'monitoring_platform',
                                          'cre90_monitoringplatformid': 'site_id',
                                          'msdyn_customerassetid': 'asset_id',
                                          'cre90_opportunityaddress': 'address',
                                          'cre90_timezone': 'time_zone'})

    # Replace Monitoring ID     <- wow, so helpful, thank you 
    df_assets = df_assets.astype({'monitoring_platform':str})
    df_assets['monitoring_platform'].replace({'662730001.0': 'Locus','662730002.0': 'PowerTrack'}, inplace = True)

    # Drop None valued rows     <- Dumbass comment, explains nothing
    # Drops rows if all values in that row are None
    df_assets_with_site_id = df_assets.dropna(subset=df_assets.columns.difference(['address']), how='all')
    df_assets_with_site_id = df_assets_with_site_id.reset_index(drop=True)

    return df_assets, df_assets_with_site_id


def get_field_names(proforma_df):
    if proforma_df['received_exception']:
        return 'KWHrec'
    elif proforma_df['net_exception']:
        return 'KWHnet'
    else:
        return 'KWHdel'


def get_proformas(session, df_assets: pd.DataFrame) -> pd.DataFrame:
    """
    Getting data from the asset table in Dynamics. Cleaning up the data slightly to 
        remove uneccessary information and formatting it.

    :param session: session for the connection to the Dynamics API
    :param df_assets: a dataframe with asset information
    :return: DataFrame with asset information and another DataFrame with same data
    """
    
    res = use_dynamics_session(session, API_URL+'cre90_assetproformas?$select=\
                                cre90_name,\
                                cre90_apistartdate,\
                                _cre90_asset_value,\
                                cre90_degradationprofile,\
                                cre90_receivedexception,\
                                cre90_netexception,\
                                cre90_januaryenergy,\
                                cre90_februaryenergy,\
                                cre90_marchenergy,\
                                cre90_aprilenergy,\
                                cre90_mayenergy,\
                                cre90_juneenergy,\
                                cre90_julyenergy,\
                                cre90_augustenergy,\
                                cre90_septemberenergy,\
                                cre90_octoberenergy,\
                                cre90_novemberenergy,\
                                cre90_decemberenergy,\
                                cre90_januaryinsolation,\
                                cre90_februaryinsolation,\
                                cre90_marchinsolation,\
                                cre90_aprilinsolation,\
                                cre90_mayinsolation,\
                                cre90_juneinsolation,\
                                cre90_julyinsolation,\
                                cre90_augustinsolation,\
                                cre90_septemberinsolation,\
                                cre90_octoberinsolation,\
                                cre90_novemberinsolation,\
                                cre90_decemberinsolation,\
                                _cre90_backupasset_value')
    res_json = res.content.decode('utf-8')
    proforma_data = json.loads(res_json)
    df_proforma = pd.DataFrame(proforma_data['value']).drop(['@odata.etag'],axis=1)
    df_proforma = df_proforma.rename(columns={'_cre90_asset_value': 'asset_id',
                                    'cre90_name': 'asset_name',
                                    'cre90_degradationprofile': 'degradation_profile',
                                    'cre90_apistartdate': 'api_start_date',
                                    'cre90_receivedexception': 'received_exception',
                                    'cre90_netexception': 'net_exception',
                                    'cre90_januaryenergy': 'jan_energy',
                                    'cre90_februaryenergy': 'feb_energy',
                                    'cre90_marchenergy': 'mar_energy',
                                    'cre90_aprilenergy': 'apr_energy',
                                    'cre90_mayenergy': 'may_energy',
                                    'cre90_juneenergy': 'jun_energy',
                                    'cre90_julyenergy': 'jul_energy',
                                    'cre90_augustenergy': 'aug_energy',
                                    'cre90_septemberenergy': 'sep_energy',
                                    'cre90_octoberenergy': 'oct_energy',
                                    'cre90_novemberenergy': 'nov_energy',
                                    'cre90_decemberenergy': 'dec_energy',
                                    'cre90_januaryinsolation': 'jan_inso',
                                    'cre90_februaryinsolation': 'feb_inso',
                                    'cre90_marchinsolation': 'mar_inso',
                                    'cre90_aprilinsolation': 'apr_inso',
                                    'cre90_mayinsolation': 'may_inso',
                                    'cre90_juneinsolation': 'jun_inso',
                                    'cre90_julyinsolation': 'jul_inso',
                                    'cre90_augustinsolation': 'aug_inso',
                                    'cre90_septemberinsolation': 'sep_inso',
                                    'cre90_octoberinsolation': 'oct_inso',
                                    'cre90_novemberinsolation': 'nov_inso',
                                    'cre90_decemberinsolation': 'dec_inso',
                                    '_cre90_backupasset_value': 'backup_asset_id'})
    df_proforma['field_name'] = df_proforma.apply(get_field_names, axis=1)
    df_proforma = df_proforma.drop(['received_exception', 'net_exception'], axis=1)
    backup_id_list = df_proforma['backup_asset_id'].tolist()
    asset_name_list = df_assets['asset_name'].tolist()
    asset_id_list = df_assets['asset_id'].tolist()
    site_id_list = df_assets['site_id'].tolist()
    backup_asset_name_list = []
    backup_site_id_list = []
    for backup_id in backup_id_list:
        if backup_id and backup_id in asset_id_list:
            index = asset_id_list.index(backup_id)
            backup_asset_name_list.append(asset_name_list[index])
            backup_site_id_list.append(site_id_list[index])
            continue
        backup_asset_name_list.append('')
        backup_site_id_list.append('')
    df_proforma['backup_asset_name'] = backup_asset_name_list
    df_proforma['backup_site_id'] = backup_site_id_list
    df_proforma = df_proforma.dropna()
    df_proforma = df_proforma.reset_index(drop=True)
    return df_proforma


def combine_assets_and_proformas(df_assets_with_site_id, df_pro):
    df_final = pd.merge(df_assets_with_site_id, df_pro, on='asset_id', how='inner')
    df_final = df_final.rename(columns={'asset_name_x': 'asset_name'}).drop(['asset_name_y'],axis=1)
    return df_final


def get_cell_by_column_name(df, asset_id, column_name):
    """Get a cell by row and column name corresponding to the asset id"""

    row_value = df.loc[df['asset_id'] == asset_id, str(column_name)].iloc[0]
    return row_value


def get_days_in_month(year, month):
    leap = 0
    thirtyone_list = [1, 3, 5, 7, 8, 10, 12]
    if month in thirtyone_list:
        return 31
    if year % 400 == 0:
        leap = 1
    elif year % 100 == 0:
        leap = 0
    elif year % 4 == 0:
        leap = 1
    if month == 2:
        return 28 + leap
    return 30


# start and end dates must be in strings
def get_degradation_factor(degradation, start_date, end_date):
    degradation = degradation / 100
    dates = rrule.rrule(rrule.DAILY,
                        dtstart=datetime.strptime(start_date, '%m-%Y'),
                        until=datetime.strptime(end_date, '%m-%Y'))
    total_days = len(list(dates))
    power = floor(total_days/365)
    degradation_factor = pow((1 - degradation), power)
    return degradation_factor


def get_asset_proforma_monthly_basis(df_final):
    """
    For each asset id monthly data is provided from start month (start date month) till end month (end date month)
    To expand this on a daily basis: each row has to be repeated according to the no. of days in that month
    """

    new_asset_id_list = []
    asset_name_list = []
    backup_asset_id_list = []
    backup_site_id_list = []
    backup_asset_name_list = []
    monitoring_platform_list = []
    site_id_list = []
    field_name_list = []
    address_list = []
    month_list = []
    year_list = []
    date_list = []
    start_date_list = []
    end_date_list = []
    degradation_profile_list = []
    days_in_month_list = []
    degradation_factor_list = []
    monthly_energy_list = []
    daily_energy_estimate_list = []
    monthly_insolation_list = []
    daily_insolation_estimate_list = []
    time_zone_list = []

    # Here end date is taken as the final day of the current year
    # (Need to update the database when a new year starts or in case of any changes to be made in the asset data)
    end_year = datetime.now().year
    end_date = '12-31-' + str(end_year)
    end_month = int(end_date.split('-')[0])
    asset_id_list = df_final['asset_id'].to_list()
    for asset_id in asset_id_list:
        start = time.time()
        start_date = get_cell_by_column_name(df_final, asset_id, 'api_start_date')
        degradation_profile = get_cell_by_column_name(df_final, asset_id, 'degradation_profile')
        asset_name = get_cell_by_column_name(df_final, asset_id, 'asset_name')
        backup_asset_id = get_cell_by_column_name(df_final, asset_id, 'backup_asset_id')
        backup_site_id = get_cell_by_column_name(df_final, asset_id, 'backup_site_id')
        backup_asset_name = get_cell_by_column_name(df_final, asset_id, 'backup_asset_name')
        monitoring_platform = get_cell_by_column_name(df_final, asset_id, 'monitoring_platform')
        site_id = get_cell_by_column_name(df_final, asset_id, 'site_id')
        field_name = get_cell_by_column_name(df_final, asset_id, 'field_name')
        address = get_cell_by_column_name(df_final, asset_id, 'address')
        time_zone = get_cell_by_column_name(df_final, asset_id, 'time_zone')
        start_month = int(start_date.split('-')[1])
        start_year = int(start_date.split('-')[0])
        new_start_date = str(start_month) + '-' + str(start_year)
        month = start_month
        year = start_year
        while month:
            new_asset_id_list.append(asset_id)
            month_list.append(month)
            year_list.append(year)
            new_end_date = str(month) + '-' + str(year)
            date_with_month_name = month_name_list[month - 1] + ' ' + str(year)
            date_list.append(date_with_month_name)
            days_in_month = get_days_in_month(year, month)
            degradation_factor = get_degradation_factor(degradation_profile, new_start_date, new_end_date)
            month_energy_col_name = month_energy_list[month - 1]
            month_insolation_col_name = month_insolation_list[month - 1]
            monthly_energy_estimate = get_cell_by_column_name(df_final, asset_id, month_energy_col_name)
            daily_energy_estimate = round((monthly_energy_estimate/days_in_month) * degradation_factor, 2)
            monthly_insolation_estimate = get_cell_by_column_name(df_final, asset_id, month_insolation_col_name)
            daily_insolation_estimate = round((monthly_insolation_estimate/days_in_month), 2)
            asset_name_list.append(asset_name)
            backup_asset_id_list.append(backup_asset_id)
            backup_site_id_list.append(backup_site_id)
            backup_asset_name_list.append(backup_asset_name)
            monitoring_platform_list.append(monitoring_platform)
            site_id_list.append(site_id)
            field_name_list.append(field_name)
            address_list.append(address)
            time_zone_list.append(time_zone)
            start_date_list.append(start_date)
            degradation_profile_list.append(degradation_profile)
            days_in_month_list.append(days_in_month)
            degradation_factor_list.append(degradation_factor)
            monthly_energy_list.append(monthly_energy_estimate)
            daily_energy_estimate_list.append(daily_energy_estimate)
            monthly_insolation_list.append(monthly_insolation_estimate)
            daily_insolation_estimate_list.append(daily_insolation_estimate)
            if month == end_month and year == end_year:
                break
            if month == 12:
                month = 1
                year += 1
                continue
            month += 1
        end = time.time()
        print(f"{asset_id}: {end - start}")

    df = pd.DataFrame({'asset_id': pd.Series(new_asset_id_list),
                       'asset_name': pd.Series(asset_name_list),
                       'backup_asset_id': pd.Series(backup_asset_id_list),
                       'backup_site_id': pd.Series(backup_site_id_list),
                       'backup_asset_name': pd.Series(backup_asset_name_list),
                       'monitoring_platform': pd.Series(monitoring_platform_list),
                       'site_id': pd.Series(site_id_list),
                       'field_name': pd.Series(field_name_list),
                       'address': pd.Series(address_list),
                       'time_zone': pd.Series(time_zone_list),
                       'month': pd.Series(month_list),
                       'year': pd.Series(year_list),
                       'date': pd.Series(date_list),
                       'start_date': pd.Series(start_date_list),
                       'days_in_month': pd.Series(days_in_month_list),
                       'degradation_profile': pd.Series(degradation_profile_list),
                       'degradation_factor': pd.Series(degradation_factor_list),
                       'monthly_energy_estimate': pd.Series(monthly_energy_list),
                       'daily_energy_estimate': pd.Series(daily_energy_estimate_list),
                       'monthly_insolation_estimate': pd.Series(monthly_insolation_list),
                       'daily_insolation_estimate': pd.Series(daily_insolation_estimate_list)})
    df = df.fillna('', inplace=False)
    # using apply function to create a new column
    df['start_year_month'] = df.apply(lambda row: str(int(row.start_date.split('-')[0])) + '-' + str(int(row.start_date.split('-')[1])), axis = 1)
    df['ref_year_month'] = df.apply(lambda row: str(row.year) + '-' + str(row.month), axis = 1)
    df.loc[(df.start_year_month == df.ref_year_month),'days_in_month'] = df.apply(lambda row: int(row.days_in_month) - int(row.start_date.split('-')[-1]) + 1, axis = 1)
    delete_column_list = ['month', 'year', 'date', 'start_year_month', 'ref_year_month']
    df = df.drop(delete_column_list, axis=1)
    return df


def proforma_monthly_dataframe():
    session = create_session()
    
    start = time.time()
    # full_asset_dataframe, asset_dataframe = get_assets(session)
    df_assets, df_assets_with_site_id = get_assets(session)
    end = time.time()
    print(f"get_asset_dataframe(session) took: {end - start}")

    start = time.time()
    proforma_dataframe = get_proformas(session, df_assets)
    end = time.time()
    print(f"get_proformas(session, full_asset_dataframe) took: {end - start}")

    start = time.time()
    combined_asset_proforma = combine_assets_and_proformas(df_assets_with_site_id, proforma_dataframe)
    end = time.time()
    print(f"combine_assets_and_proformas(asset_dataframe, proforma_dataframe) took: {end - start}")
    # now = get_time_now()
    # combined_asset_proforma.to_csv(f"csvs/combined_asset_proforma_{now}.csv")

    start = time.time()
    asset_proforma_monthly_basis = get_asset_proforma_monthly_basis(combined_asset_proforma)
    asset_proforma_monthly_basis['monitoring_platform'].replace({'662730001.0': 'Locus', '662730002.0': 'PowerTrack'}, inplace=True)
    # maintain dataframe for unique assets
    asset_proforma_monthly_basis = asset_proforma_monthly_basis.drop_duplicates('asset_id', keep='first')
    end = time.time()
    print(f"get_asset_proforma_monthly_basis(combined_asset_proforma) took: {end - start}")
    # now = get_time_now()
    # asset_proforma_monthly_basis.to_csv(f"csvs/asset_proforma_monthly_basis_{now}.csv")

    return asset_proforma_monthly_basis


# if __name__ == '__main__':
#     proforma_df = proforma_monthly_dataframe()
