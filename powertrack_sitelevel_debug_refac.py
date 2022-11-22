#!/usr/bin/env python
# coding: utf-8

'''
Notes for Dr. Santosh

This script uses the PowerTrack API to download all relevant datapoints for all sites

Similarly to the other script, the main for loop is near the bottom of the script and controls the flow of the script

This API requires very specific formatting of the headers and payload. Please see the get_site_hardware(site_id) and pull_meter_production()
functions for examples of this.

This script has logging enabled

This script connects to databases on the local machine; you will need to change this

This script assumes the existence of a "Pro Forma" database containing expected energy data

The compile_meter_data function aggregates the energy data from all meters and sums it up to site level

This script only aggregates data to site level, not meter/inverter
'''



import requests
import json
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
import smartsheet
import datetime
from datetime import timedelta
import pandas as pd
import shutil
import logging
import sys
import numpy as np

import sqlalchemy as db
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey, text
from sqlalchemy import select, join, and_, or_, desc, asc, between, func

#DEFINE FUNCTIONS


def format_date(date):
    return date.strftime("%m/%d/%Y")

def format_datetime(date):
    return date.strftime("%Y-%m-%dT%X")

def convert_datetime_to_timestamp_plus_timezone(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S')+"+00:00"

def convert_datetime_to_date_timestamp(dt):
    return dt.strftime("%Y-%m-%d")

def parse_date(date_string):
    return datetime.datetime.strptime(date_string, "%Y-%m-%d")

def parse_datetime(date_string):
    return datetime.datetime.strptime(date_string, "%m/%d/%Y")

def parse_datetime_plus_time(date_string):
    return datetime.datetime.strptime(date_string, "%Y-%m-%dT%X")

def parse_datetime_plus_timezone(date_string):
    return datetime.datetime.strptime(date_string, "%Y-%m-%dT%X+00:00")

def remove_time_from_timestamp(timestamp):
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%X+00:00")
    return dt.strftime("%Y-%m-%d")

def convert_pto_to_timestamp(pto_date):
    pto_datetime = datetime.datetime.strptime(pto_date, '%Y-%m-%d')
    pto_timestamp = pto_datetime.strftime('%Y-%m-%dT%H:%M:%S')+"+00:00"
    return pto_timestamp

def prepare_start_date(pto_date):
    start_dt = parse_datetime_plus_time(pto_date)
    formatted_dt = format_datetime(start_dt - timedelta(days = 1) + timedelta(hours = 8))
    
    return formatted_dt

def find_site_id(target_project_id):
    for site_id, project_id in reference_project_id_map.items():
        if(str(project_id) == str(target_project_id)):
            return site_id

def find_field_name(site_id):
    if(site_id in received_site_ids):
        return "KWHrec"
    elif(site_id in net_site_ids):
        return "KWHnet"
    else:
        return "KWHdel"

def create_pto_date_dictionary():
    #set up a dictionary to link project ID to PTO date
    pto_date_dict = {}
    for row in equipment_sheet.rows:
        pto_date = get_equipment_cell_by_column_name(row, "PTO Date").value
        project_id = get_equipment_cell_by_column_name(row, "Project ID").value

        if(type(project_id) == float):
            project_id = str(int(project_id))

        if(project_id != "TEMPLATE" and pto_date is not None):
            pto_date_string = datetime.datetime.strptime(pto_date, "%Y-%m-%d").strftime("%m/%d/%Y")
            pto_date_dict[project_id] = pto_date_string
            
    return pto_date_dict

def create_backup_insolation_dictionary():
    #set up a dictionary to link project ID to backup project ID
    backup_insolation_dict = {}
    for row in equipment_sheet.rows:
        backup_project_id = get_equipment_cell_by_column_name(row, "Backup Weather Station").value
        project_id = get_equipment_cell_by_column_name(row, "Project ID").value

        if(type(project_id) == float):
            project_id = str(int(project_id))
        
        if(type(backup_project_id) == float):
            backup_project_id = str(int(backup_project_id))

        if(project_id != "TEMPLATE" and backup_project_id is not None):
            backup_insolation_dict[project_id] = backup_project_id
            
    return backup_insolation_dict

def datetime_filename():
    date = datetime.datetime.now()
    date_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    db_string = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Backup/PowerTrack "
    
    final_string = db_string + date_string + ".db"
    
    return final_string

def log_filename():
    date = datetime.datetime.now()
    date_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    db_string = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus/Jupyter Files/PowerTracker/Logs/PowerTrack Log - "
    
    final_string = db_string + date_string + ".log"
    
    return final_string

def connect_to_pro_forma_db():

    database_path = 'sqlite:///C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus Pro Forma.db'

    pro_forma_engine = create_engine(database_path)

    #pro_forma_metadata = MetaData(pro_forma_engine)

    pro_forma_metadata = MetaData()
    pro_forma_metadata.reflect(bind = pro_forma_engine)

    pro_forma_table = pro_forma_metadata.tables['pro_forma']

    #pro_forma_table = Table('pro_forma', pro_forma_metadata, autoload=True)

    #pro_forma_table = Table('pro_forma', pro_forma_metadata, autoload=True, autoload_with=pro_forma_engine)

    pro_forma_conn = pro_forma_engine.connect()

    return pro_forma_table, pro_forma_conn

def pull_pro_forma(project_id):

    sql_select_statement = "select * from pro_forma where project_id = '{}'".format(project_id)

    result = pro_forma_conn.execute(sql_select_statement).fetchall()

    pro_forma_df = pd.DataFrame(result)

    pro_forma_df.columns = ["project_id", "project_name", "month", "month_name", "year", "day", "pro_forma"]
    
    months = list(pro_forma_df["month"])
    years = list(pro_forma_df["year"])
    days = list(pro_forma_df["day"])

    timestamps = [datetime.datetime(int(year), int(month), int(day)).strftime('%Y-%m-%d')
                for year, month, day in zip(years, months, days)]

    pro_forma_df.index = timestamps

    pro_forma_df = pro_forma_df.drop(["project_name", "month", "month_name", "year", "day", "project_id"], axis=1)

    return pro_forma_df

def find_pto_data(project_pro_forma_df):
    #find pto data from project pro forma df
    pto_year = int(project_pro_forma_df.at[list(project_pro_forma_df.index)[0], "year"])
    pto_month_name = project_pro_forma_df.at[list(project_pro_forma_df.index)[0], "month"]
    pto_month = month_names.index(pto_month_name) + 1
    
    return pto_year, pto_month, pto_month_name

def collect_expected_insolation_data():
    project_ids, years, months, insolations = [],[],[],[]
    for i in range(len(sheet.rows)):
        row = sheet.rows[i]

        project_id = get_cell_by_column_name(row, "Project ID").value
        year = get_cell_by_column_name(row, "Year").value
        month = get_cell_by_column_name(row, "Month").value
        insolation = get_cell_by_column_name(row, "Expected Insolation").value

        if(type(project_id) == float):
            project_id = str(int(project_id))
        else:
            project_id = str(project_id)
        
        project_ids.append(project_id)
        years.append(year)
        months.append(month)
        insolations.append(insolation)

    insolation_dict = {"project_id": project_ids, "year": years, "month": months, "expected_insolation": insolations}
    insolation_df = pd.DataFrame(insolation_dict)

    return insolation_df

def extract_project_insolation_rows(project_id, insolation_df):
    #extract relevant pro forma df rows
    project_ids = list(insolation_df["project_id"])
    project_index = project_ids.index(project_id)
    insolations, years, months = [],[],[]
    for i in range(project_index, project_index+12):
        insolation = insolation_df.at[i, "expected_insolation"]
        year = insolation_df.at[i, "year"]
        month = insolation_df.at[i, "month"]
        
        months.append(month)
        years.append(year)
        insolations.append(insolation)
        
    insolation_dict = {"year": years, "month": months, "expected_insolation": insolations}
    project_insolation_df = pd.DataFrame(insolation_dict)
    
    project_insolation_df.index = months
    
    return project_insolation_df

def pull_and_extrapolate_insolation(timestamps, expected_insolation_df, project_id):
    #execute three successive functions to pull and calculate project pro forma
    if(str(project_id) in list(expected_insolation_df["project_id"])):
        project_insolation_df = extract_project_insolation_rows(project_id, expected_insolation_df)

        pto_year, pto_month, pto_month_name = find_pto_data(project_insolation_df)

        new_insolation_df = extrapolate_all_project_insolation(timestamps, project_insolation_df, pto_month, pto_year)
    
        return new_insolation_df
    else:
        return pd.DataFrame({"Insolation": []})

def extrapolate_insolation_datapoint(month, year, pto_month, pto_year, insolation):
    #extrapolate a single pro forma datapoint
    if(month < pto_month and year <= pto_year):
        #Prior to PTO Date, return 0
        return 0
    if(year < pto_year):
        #Prior to PTO Date, return 0
        return 0
    
    return insolation/month_lengths[month]

def extrapolate_all_project_insolation(timestamps, project_insolation_df, pto_month, pto_year):
    #calculate and save all pro forma for a project
    insolations = []
    for timestamp in timestamps:
        date = datetime.datetime.strptime(timestamp, "%Y-%m-%d")
        day = date.day
        month = date.month
        month_name = date.strftime("%b")
        year = date.year
        
        insolation = project_insolation_df.at[month_name, "expected_insolation"]
        
        new_insolation = extrapolate_insolation_datapoint(month, year, pto_month, pto_year, insolation)
        
        insolations.append(new_insolation)
    
    expected_insolation_df = pd.DataFrame({"expected_insolation": insolations})
    expected_insolation_df.index = timestamps

    return expected_insolation_df

def extract_project_insolation(site_id, project_id, timestamps):
    if(site_id in reference_project_id_map):
        print("Retrieving and extrapolating expected insolation...")
        #retrieve and extrapolate pro forma data
        project_insolation_df = pd.DataFrame({"expected_insolation": [0.0]*len(production_df)})
        project_insolation_df.index = production_df.index
        #project_insolation_df = pd.concat([production_df, project_insolation_df], axis=1)
        project_insolation_df = pull_and_extrapolate_insolation(timestamps, expected_insolation_df, project_id)
        print("Expected insolation retrieved.")
    else:
        print("No expected insolation data found :/")
        project_insolation_df = pd.DataFrame({"expected_insolation":[]})
    
    return project_insolation_df

def create_project_id_maps():
    #define a function to pull out project_id_map for Locus and PowerTrack
    project_id_map, powertrack_project_id_map = {},{}
    for row in equipment_sheet.rows:
        platform = get_equipment_cell_by_column_name(row, "Monitoring Platform").value
        site_id = get_equipment_cell_by_column_name(row, "Monitoring Platform ID").value
        pegasus = get_equipment_cell_by_column_name(row, "Pegasus").value
        project_id = get_equipment_cell_by_column_name(row, "Project ID").value

        if(type(project_id) == float):
            project_id = int(project_id)
        if(type(site_id) == float):
            site_id = int(site_id)

        if(pegasus == "Yes"):
            if(platform == "Locus"):
                #if site id already exists, add projects together in a list
                if(site_id in project_id_map):
                    value = project_id_map[site_id]
                    if(type(value) == list):
                        value.append(project_id)
                        project_id_map[site_id] = value
                    else:
                        value_list = [value, project_id]
                        project_id_map[site_id] = value_list
                else:
                    project_id_map[site_id] = project_id
            elif(platform == "PowerTrack"):
                powertrack_project_id_map[site_id] = project_id
    
    return project_id_map, powertrack_project_id_map

def request_access_token():
    oauth = OAuth2Session(client=LegacyApplicationClient(client_id = client_id))

    token_response = oauth.fetch_token(token_url = "https://api.alsoenergy.com/Auth/token", username = username,
                                       password = password, client_id = client_id, client_secret = client_secret)

    token = token_response['access_token']
    
    return token

def compile_site_list(token):
    #compile a list of all sites and site IDs
    url = "https://api.alsoenergy.com/Sites"
    
    payload = {}
    headers = {
        'Authorization': 'Bearer ' + token
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    
    response_json = json.loads(response.text)
    
    sites = {}
    for site in response_json['items']:
        site_name = site['siteName']
        site_id = site['siteId']

        sites[site_id] = site_name
        
        print(site_name, site_id)
    
    return sites

def retrieve_project_details(site_id):
    pto_date = start_date
    
    if(site_id in reference_project_id_map):
        #assign project ID
        project_id = str(reference_project_id_map[site_id])
        
        #retrieve project PTO date from Equipment Library smartsheet
        if(project_id in pto_date_dict.keys()):
            pto_datetime = parse_datetime(pto_date_dict[project_id])
            pto_date = format_datetime(pto_datetime)
    
        #retrieve backup insolation site
        if(project_id in backup_insolation_dict):
            backup_insolation_id = backup_insolation_dict[project_id]
        else:
            backup_insolation_id = None
    
    return project_id, pto_date, pto_datetime, backup_insolation_id

def get_site_hardware(site_id):
    #retrieve site hardware list
    url = "https://api.alsoenergy.com/Sites/{}/Hardware"
    url = url.format(str(site_id))
    
    payload = {}
    headers = {
        'Authorization': 'Bearer ' + token
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    
    response_json = json.loads(response.text)
    
    #compile a list of all hardware for the site
    hardware_list = {}
    for hardware in response_json['hardware']:
        hardware_details = (hardware['functionCode'], hardware['name'])
        hardware_list[hardware['id']] = hardware_details
    
    return hardware_list

def get_site_meters_and_weather_stations(hardware):
    meters, weather_stations = {},{}
    for hardware_id in hardware:
        function_code = hardware[hardware_id][0]
        name = hardware[hardware_id][1]
        
        if(function_code in meter_codes):
            meters[hardware_id] = name
        if(function_code in weather_station_codes):
            weather_stations[hardware_id] = name
    
    return meters, weather_stations

def pull_meter_production(token, site_id, hardware_id, start_date, end_date, field_name):
    
    #add one day to start and end dates
    start_date = format_datetime(parse_datetime_plus_time(start_date) + timedelta(days = 1))
    end_date = format_datetime(parse_datetime_plus_time(end_date) + timedelta(days = 1))

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json-patch+json',
        'Authorization': 'Bearer ' + token
    }

    params = (
        ('from', start_date),
        ('to', end_date),
        ('binSizes', 'BinDay'),
    )

    #prepare "fields" parameter
    data = '[ { "hardwareId": ' + str(hardware_id) + ', '
    data += '"siteId": ' + str(site_id) + ', '
    data += '"fieldName": "' + str(field_name) + '", "function": "Diff" }]'
    
    response = requests.post('https://api.alsoenergy.com/v2/Data/BinData', headers=headers, params=params, data=data)
        
    response_json = json.loads(response.text)
        
    return response_json

def pull_weather_station_insolation(token, site_id, hardware_id, start_date, end_date):
    
    #add one day to start and end dates
    start_date = format_datetime(parse_datetime_plus_time(start_date) + timedelta(days = 1))
    end_date = format_datetime(parse_datetime_plus_time(end_date) + timedelta(days = 1))

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json-patch+json',
        'Authorization': 'Bearer ' + token
    }

    params = (
        ('from', start_date),
        ('to', end_date),
        ('binSizes', 'BinDay'),
    )

    #prepare "fields" parameter
    data = '[ { "hardwareId": ' + str(hardware_id) + ', '
    data += '"siteId": ' + str(site_id) + ', '
    data += '"fieldName": "Sun", "function": "Integral" }]'
    
    response = requests.post('https://api.alsoenergy.com/v2/Data/BinData', headers=headers, params=params, data=data)
        
    response_json = json.loads(response.text)
        
    return response_json

def compile_meter_data(token, site_id, meters, start_date, end_date, field_name):
    print("Compiling meter data...")
    #loop over all meters
    meter_dfs = []
    for meter in list(meters.keys()):
        meter_name = meters[meter]
        
        print("Pulling data for meter " + meter_name)
        #make an api call to pull meter production data
        print("Start Date", start_date)
        response_json = pull_meter_production(token, site_id, meter, start_date, end_date, field_name)
        
        #loop over all dates and retrieve production data
        timestamps, datapoints = [],[]
        for item in response_json['items']:
            timestamp = item['timestamp']
            datapoint = item['data'][0]

            #remove time from timestamp
            formatted_timestamp = remove_time_from_timestamp(timestamp)

            timestamps.append(formatted_timestamp)
            datapoints.append(datapoint)
        
        #create a dataframe from datapoints with timestamps as index
        meter_df = pd.DataFrame({meter_name: datapoints})
        meter_df.index = timestamps

        # 1_5_2022 DELETE
        meter_df = meter_df[~meter_df.index.duplicated(keep='last')]
        # 1_5_2022 DELETE

        #store the dataframe for later
        meter_dfs.append(meter_df)
    
    # # 1_5_2022 DELETE
    for idx, df in enumerate(meter_dfs):
        df = df[~df.index.duplicated(keep='last')]
        now = datetime.datetime.now()
        now = now.strftime("%Y-%m-%d %H-%M-%S")
        df.to_csv(f"C:\\Users\\NadimAtalla\\Desktop\\to-csvs\\powertrack\\meter_{site_id}_{idx}_df_{now}.csv")
    # # 1_5_2022 DELETE
    
    #combine all meter dataframes into one
    production_df = pd.concat(meter_dfs, axis = 1, sort = True)
        
    #sum all meter dataframes to produce total
    production_sum = pd.DataFrame(production_df.sum(axis = 1))
    production_sum.columns = ["measured_energy"]
    
    #combine into final production dataframe
    # how = 'left'
    # if len(production_sum.index) > len(production_df.index): how = 'right'
    # final_production_df = pd.merge(production_df, production_sum, left_index=True, right_index=True, how=how, sort=True)   UNCOMMENT ME IF MERGE ^ FAILS
    # production_sum.drop_duplicates(keep='last', inplace=True)   # See how this works
    production_sum = production_sum[~production_sum.index.duplicated(keep='last')]
    final_production_df = pd.concat([production_df, production_sum], axis = 1, sort = True)
    
    print("Meter data compiled.")
    
    return final_production_df

def get_site_insolation(site_id, weather_stations):
    
    print("Compiling insolation...")
    
    #loop over all weather stations
    ws_dfs, non_null_counts, ws_details = [],[],[]
    
    for weather_station in list(weather_stations.keys()):
        ws_name = weather_stations[weather_station]
        try:
            #make an api call to pull weather station insolation data
            response_json = pull_weather_station_insolation(token, site_id, weather_station, start_date, end_date)

            #loop over all dates and retrieve production data
            timestamps, datapoints = [],[]
            for item in response_json['items']:
                timestamp = item['timestamp']
                datapoint = item['data'][0]

                formatted_timestamp = remove_time_from_timestamp(timestamp)

                #convert to kWh
                if(type(datapoint) == float or type(datapoint) == int):
                    datapoint = datapoint/1000

                timestamps.append(formatted_timestamp)
                datapoints.append(datapoint)

            #create a dataframe from datapoints with timestamps as index
            ws_df = pd.DataFrame({ws_name: datapoints})
            ws_df.index = timestamps

            null_count = 0
            for datapoint in datapoints:
                if(pd.isna(datapoint)):
                    null_count += 1

            non_null_count = len(datapoints) - null_count

            #store the dataframe and count for later
            ws_dfs.append(ws_df)
            non_null_counts.append(non_null_count)
            ws_details.append((weather_station, ws_name))
            print("POA Insolation pulled from weather station: ", ws_name)
        except:
            print("No data for weather station " + ws_name)
    
    if(len(ws_dfs) > 1):
        #ws_df_lengths = [len(ws_df) for ws_df in ws_dfs]
        
        max_length = max(non_null_counts)
        max_index = non_null_counts.index(max_length)
        
        #all_ws_dfs = pd.concat(ws_dfs, axis = 1, sort = True)
        
        insolation_df = ws_dfs[max_index]
        insolation_df.columns = ["poa_insolation"]

        ws_detail = ws_details[max_index]
    elif(len(ws_dfs) == 1):
        insolation_df = ws_dfs[0]
        insolation_df.columns = ["poa_insolation"]

        ws_detail = ws_details[0]
    else:
        insolation_df = pd.DataFrame({"poa_insolation": []})
        ws_detail = ("", "")
    
    print("Insolation compiled.")
    
    return insolation_df, ws_detail

def extract_backup_insolation(backup_insolation_id, timestamps):
    
    site_id = find_site_id(backup_insolation_id)  
    
    #retrieve site hardware list
    hardware = get_site_hardware(site_id)
    
    #retrieve site meters
    _, weather_stations = get_site_meters_and_weather_stations(hardware)
    
    #extract project measured insolation
    insolation_df, ws_detail = get_site_insolation(site_id, weather_stations)
    insolation_df.columns = ["backup_poa_insolation"]
    
    #extract site expected insolation if it exists
    project_expected_insolation_df = extract_project_insolation(site_id, backup_insolation_id, timestamps)
    project_expected_insolation_df.columns = ["backup_expected_insolation"]
    
    return insolation_df, project_expected_insolation_df, ws_detail

def find_timestamp_range(pto_date):
    #find all timestamps for this project's lifetime
    first_datetime = datetime.datetime(pto_date.year, pto_date.month, pto_date.day, 0,0,0)

    current_datetime = datetime.datetime.now()
    
    final_datetime = datetime.datetime(current_datetime.year, current_datetime.month, current_datetime.day, 0,0,0)

    number_of_days = (final_datetime - first_datetime).days

    all_timestamps, all_datetimes = [],[]
    for i in range(number_of_days):
        dt = first_datetime + timedelta(days=i)
        timestamp = convert_datetime_to_date_timestamp(dt)
        all_timestamps.append(timestamp)
        all_datetimes.append(dt)
    
    return all_timestamps, all_datetimes

def format_final_dataframe(final_df, df_list, project_id, site_id, timestamps):
    
    years, months, month_names_list, days = [],[],[],[]
    for timestamp in timestamps:
        dt = parse_date(timestamp)

        year = dt.year
        month = dt.month
        day = dt.day

        month_name = month_names[month - 1]

        years.append(str(np.round(year, 0)))
        months.append(str(np.round(month, 0)))
        month_names_list.append(month_name)
        days.append(str(np.round(day, 0)))

    project_ids = [project_id]*len(timestamps)
    site_ids = [site_id]*len(timestamps)

    format_df = pd.DataFrame({"project_id": project_ids, "site_id": site_ids, "year": years,
                              "month": months, "day": days, "month_name": month_names_list})    
    format_df.index = timestamps

    # 1_5_2022 DELETE
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H-%M-%S")
    final_df.to_csv(f"C:\\Users\\NadimAtalla\\Desktop\\to-csvs\\powertrack\\final_df_{site_id}_{now}.csv")
    format_df.to_csv(f"C:\\Users\\NadimAtalla\\Desktop\\to-csvs\\powertrack\\format_df_{site_id}_{now}.csv")

    format_df = format_df[~format_df.index.duplicated(keep='last')]
    final_df = final_df[~final_df.index.duplicated(keep='last')]
    final_df = pd.concat([format_df, final_df], axis=1, sort=False) 
    # 1_5_2022 DELETE

    extra_timestamps = []
    for df in df_list:
        df_timestamps = list(df.index)
        # 1_5_2022 DELETE
        df = df[~df.index.duplicated(keep='last')]
        final_df = pd.concat([final_df, df], axis=1, sort=False)
        # 1_5_2022 DELETE
        df_extra_timestamps = [item for item in df_timestamps if item not in timestamps]
        extra_timestamps = extra_timestamps + df_extra_timestamps

    extra_timestamps = list(set(extra_timestamps))

    for column in final_df.columns:
        if(column not in column_names):
            final_df = final_df.drop(column, axis=1)

    #remove the extra rows coming from Pro Forma DB
    try:
        final_df = final_df.drop(extra_timestamps, axis=0, errors='ignore')
        print("Extra Timestamps deleted...")
    except:
        print("Extra  Timestamps included...")

    # 4_4_2022 DELETE
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H-%M-%S")
    final_df.to_csv(f"C:\\Users\\NadimAtalla\\Desktop\\to-csvs\\powertrack\\final_df_LAST_{site_id}_{project_id_map[site_id]}_{now}.csv")

    return final_df

def create_database(database_path):
    #Create database engine
    engine = create_engine(database_path, echo = False)
    meta = MetaData()

    #Drop table if it already exists
    sql = text('DROP TABLE IF EXISTS energy;')
    result = engine.execute(sql)

    #create energy table
    energy = Table(
        'energy', meta,
        Column('project_id', String),
        Column('site_id', String),
        Column('month_name', String),
        Column('month', String),
        Column('year', String),
        Column('day', String),
        Column('measured_energy', Float),
        Column('pro_forma', Float),
        Column('poa_insolation', Float),
        Column('expected_insolation', Float),
        Column('backup_poa_insolation', Float),
        Column('backup_expected_insolation', Float)
    )

    meta.create_all(engine)

    #connect to engine
    conn = engine.connect()
    
    return energy, conn

def insert_into_database(project_id, site_id, final_df):
    for row in final_df.index:
        month_name = final_df.at[row, "Month Name"]
        month = final_df.at[row, "Month"]
        year = final_df.at[row, "Year"]
        day = final_df.at[row, "Day"]
        pro_forma = final_df.at[row, "pro_forma"]
        measured_energy = final_df.at[row, "Total Energy Delivered"]
        poa_insolation = final_df.at[row, "POA Insolation"]
        expected_insolation = final_df.at[row, "Expected Insolation"]
        backup_poa_insolation = final_df.at[row, "Backup POA Insolation"]
        backup_expected_insolation = final_df.at[row, "Backup Expected Insolation"]
        
        s = energy.insert().values(project_id = project_id, site_id = site_id, month_name = month_name, month = month,
                                   year = year, day = day, pro_forma = pro_forma, measured_energy = measured_energy,
                                   poa_insolation = poa_insolation, expected_insolation = expected_insolation,
                                   backup_poa_insolation = backup_poa_insolation,
                                   backup_expected_insolation = backup_expected_insolation)

        conn.execute(s)



#DEFINE GLOBAL VARIABLES

script_start_time = datetime.datetime.now()


password = "998thLCoftheNW"
username = "safariapi"
client_id = "5b7bc63684c19388e1d253565cb99980"
client_secret = "c653512c7190315d6ecf008da9ee3f33"
meter_codes = ["PM", "CM", "GM", "XM", "WT"]
weather_station_codes = ["WS"]

month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
month_lengths = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

start_date = format_datetime(datetime.datetime.now() - timedelta(100))

end_date = format_datetime(datetime.datetime.now() - timedelta(1))

bin_size = "BinDay"

field_function = "Diff"

received_site_ids = [33533, 35295, 51583, 51019, 51411, 51052, 51854, 51820, 51195, 51566, 51074, 51196, 51197, 
                     52031, 51194, 51053, 52029, 52030, 51546, 51644, 51858, 51547, 51819, 51989, 53177, 51222, 53571, 53076]

net_site_ids = [39684]

project_id_map = {33184:4060, 33533:3877, 39830:2841, 43050:2840, 35295:"1880a", 38054:"1880b", 35401:2843,
                  39684:1364, 50766:3936, 51013:3937, 50717:4549, 50944:4558, 51049:4553, 51019:4554, 51411:4551,
                  51583:4783, 51050:4550, 50718:4556, 51854:4454, 51052:4453}

#Hertz-Estero (Weather Station Issues)
#33692:14

column_names = ['project_id', 'site_id', 'month_name', 'month', 'year', 'day', 'measured_energy','pro_forma',
                'poa_insolation', 'expected_insolation', 'backup_poa_insolation', 'backup_expected_insolation']


#SET UP SMARTSHEET



#set up the Smartsheet client and pull the  sheet
ss_client = smartsheet.Smartsheet('xk6z68j6qf4gu6a40c1uozevbo')
sheet_id = 1341629924697988

#pull the Pro Forma smartsheet
sheet = ss_client.Sheets.get_sheet(sheet_id)

#instantiate and populate a dictionary linking column names to IDs
column_map = {}
for column in sheet.columns:
        column_map[column.title] = column.id

#define a function to get a cell by row and column name
def get_cell_by_column_name(row, column_name):
    column_id = column_map[column_name]
    return row.get_column(column_id)

equipment_sheet_id = 846725138147204

#pull the equipment library smartsheet
equipment_sheet = ss_client.Sheets.get_sheet(equipment_sheet_id)

#instantiate and populate a dictionary linking column names to IDs
equipment_column_map = {}
for column in equipment_sheet.columns:
        equipment_column_map[column.title] = column.id

#define a function to get a cell by row and column name
def get_equipment_cell_by_column_name(row, column_name):
    column_id = equipment_column_map[column_name]
    return row.get_column(column_id)

#create a dictionary between project ID and PTO date
pto_date_dict = create_pto_date_dictionary()

#create a dictionary between project ID and backup project ID
backup_insolation_dict = create_backup_insolation_dictionary()

#create a dictionary of expected insolation
expected_insolation_df = collect_expected_insolation_data()

#create project_id_map from Equipment Library
_, project_id_map = create_project_id_maps()

reference_project_id_map = project_id_map.copy()

#CODE BODY

#setup logging filename
log_name = log_filename()

#set up a logger to log messages and errors
logger = logging.getLogger()
logging.basicConfig(filename = log_name, level = logging.INFO)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
fh = logging.FileHandler(filename=log_name)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

#log all printed text to log file
log = open(log_name, "a")
sys.stdout = log

#request OAuth 2.0 token
print("Requesting access token...")
token = request_access_token()

#compile a list of all Safari sites on PowerTrack
sites = compile_site_list(token)

#set up the energy database
database_path = 'sqlite:///C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/PowerTrack.db'
print("Creating database...")
energy, conn = create_database(database_path)

#connect to pro forma db
pro_forma_table, pro_forma_conn = connect_to_pro_forma_db()

#loop over all relevant sites
final_dfs = {}
project_id_map = { 60315: 6087, 60314: 6086, 60316: 6085 }
print(project_id_map)

for site_id in project_id_map:
    try:
        site = sites[site_id]

        print("Working on: " + site, site_id)

        #check if project has project ID, PTO date, and backup insolation
        project_id, start_date, start_datetime, backup_insolation_id = retrieve_project_details(site_id)

        print(project_id, backup_insolation_id)

        #find the beginning and end times appearing in Locus data
        all_timestamps, all_datetimes = find_timestamp_range(start_datetime)

        #fix start date to pull from correct datetime for PowerTrack
        start_date = prepare_start_date(start_date)

        #retrieve site hardware list
        hardware = get_site_hardware(site_id)

        #retrieve site meters
        meters, weather_stations = get_site_meters_and_weather_stations(hardware)

        #find the field name: KWHdel or KWHnet
        field_name = find_field_name(site_id)

        #compile project meter level production data
        production_df = compile_meter_data(token, site_id, meters, start_date, end_date, field_name)

        #extract project measured insolation
        insolation_df, ws_detail = get_site_insolation(site_id, weather_stations)
        print("Weather station for project", project_id, ":", ws_detail)

        #extract site Pro Forma if it exists
        project_pro_forma_df = pull_pro_forma(project_id)

        #extract site expected insolation if it exists
        project_expected_insolation_df = extract_project_insolation(site_id, project_id, all_timestamps)

        #extract backup insolation and pro forma
        backup_insolation_df, backup_expected_insolation_df, backup_ws_detail = extract_backup_insolation(backup_insolation_id, all_timestamps)
        print("Backup weather station for project", project_id, ":", backup_ws_detail)

        #combine all dataframes into one final one
        final_df = format_final_dataframe(production_df, [project_pro_forma_df, insolation_df, project_expected_insolation_df,
                                      backup_insolation_df, backup_expected_insolation_df], project_id, site_id, all_timestamps)

        #store final_df in master dictionary
        final_dfs[project_id] = final_df

        print("Inserting into database...")
        #insert project into database
        final_df.to_sql("energy", conn, if_exists='append', index=False)

        #insert project data into database
        
        #insert_into_database(project_id, site_id, final_df)
        
    except Exception as e:
        print(e)
        #log any errors that occur during script run
        print("Error in site: " + site)
        logger.exception("Error")




print("Saving PowerTrack DB Backup...")
source = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/PowerTrack.db"

destination = datetime_filename()

# shutil.copy(source, destination)

conn.close()

#calculate time taken for entire script
script_end_time = datetime.datetime.now()
time_taken = (script_end_time - script_start_time).seconds
print("Script runtime: ", time_taken, " seconds")

print("Script computation complete!")

