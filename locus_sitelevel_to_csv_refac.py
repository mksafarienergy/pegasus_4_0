#!/usr/bin/env python
# coding: utf-8

# In[1]:


#IMPORTS


# In[2]:


import sqlalchemy as db
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey, text
from sqlalchemy import select, join, and_, or_, desc, asc, between, func


# In[3]:


import requests
import json
from datetime import datetime
from datetime import timedelta
from datetime import date
import pandas as pd
import numpy as np
import smartsheet
import shutil
import sys
import logging
import smtplib

#benchmark script time
script_start_time = datetime.now()
database_df = pd.DataFrame() 


# In[4]:


#FUNCTIONS


# In[5]:


def extract_year_month_day(timestamp):
    year = int(timestamp[0:4])
    month = int(timestamp[5:7])
    day = int(timestamp[8:10])
    return year, month, day


# In[6]:


def format_datetime(current_datetime):
    #turns datetime objects into strings adhering to the Locus API format
    datetime_string = ""
    
    year = str(current_datetime.year)
    month = str(current_datetime.month)
    if(len(month) == 1):
        month = "0" + month
    day = str(current_datetime.day)
    if(len(day) == 1):
        day = "0" + day
    hour = str(current_datetime.hour)
    if(len(hour) == 1):
        hour = "0" + hour
    minute = str(current_datetime.minute)
    if(len(minute) == 1):
        minute = "0" + minute
    second = str(current_datetime.second)
    if(len(second) == 1):
        second = "0" + second
    
    datetime_string = year + "-" + month + "-" + day + "T" + hour + ":" + minute + ":" + second
    
    return datetime_string


# In[7]:


def format_timestamp(current_datetime):
    #turns datetime objects into strings adhering to the Locus API format
    datetime_string = ""
    
    year = str(current_datetime.year)
    month = str(current_datetime.month)
    if(len(month) == 1):
        month = "0" + month
    day = str(current_datetime.day)
    if(len(day) == 1):
        day = "0" + day
    hour = str(current_datetime.hour)
    if(len(hour) == 1):
        hour = "0" + hour
    minute = str(current_datetime.minute)
    if(len(minute) == 1):
        minute = "0" + minute
    second = str(current_datetime.second)
    if(len(second) == 1):
        second = "0" + second
    
    datetime_string = year + "-" + month + "-" + day + "T" + hour + ":" + minute + ":" + second + "+00:00"
    
    return datetime_string


# In[8]:


def find_previous_month():
    #finds the start and end datetimes of the previous month
    current_datetime = datetime.now()
    current_month = current_datetime.month
    current_year = current_datetime.year
    
    first_of_month = datetime(current_year, current_month, 1)
    end_of_previous_month = first_of_month - timedelta(days = 1)
    
    start_of_previous_month = datetime(end_of_previous_month.year, end_of_previous_month.month, 1)
    
    end_of_previous_month = datetime(end_of_previous_month.year, end_of_previous_month.month, end_of_previous_month.day,
                                    23, 59, 59)
    
    start_of_previous_month = datetime(start_of_previous_month.year, start_of_previous_month.month, start_of_previous_month.day,
                                      0, 0, 0)
    
    return start_of_previous_month, end_of_previous_month


# In[9]:


def find_previous_day():
    #finds the datetime of the previous day
    current_datetime = datetime.now()
    
    start_of_today = datetime(current_datetime.year, current_datetime.month, current_datetime.day, 0,0,0)
    
    start_of_interval = start_of_today - timedelta(days = 1825)
    
    return start_of_interval, start_of_today


# In[10]:


def datetime_filename():
    #create a database filename based on the date
    date = datetime.now()
    date_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    db_string = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Backup/Pegasus Beta 1.0 - "
    
    final_string = db_string + date_string + ".db"
    
    return final_string


# In[11]:


def log_filename():
    #create a log filename based on the date
    date = datetime.now()
    date_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    db_string = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Logs/Pegasus Log - "
    
    final_string = db_string + date_string + ".log"
    
    return final_string

def debug_log_filename():
    #create a log filename based on the date
    date = datetime.now()
    date_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    db_string = "C:/Users/NadimAtalla/Desktop/temp/Pegasus Log - "
    
    final_string = db_string + date_string + ".log"
    
    return final_string


# In[12]:


def meter_datetime_filename():
    #create a meter level database filename based on the date
    date = datetime.now()
    date_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    db_string = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Backup/Pegasus Meter Level 1.0 - "
    
    final_string = db_string + date_string + ".db"
    
    return final_string

def inverter_datetime_filename():
    #create a meter level database filename based on the date
    date = datetime.now()
    date_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    db_string = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Backup/Pegasus Inverter Level 1.0 - "
    
    final_string = db_string + date_string + ".db"
    
    return final_string


# In[13]:


def convert_timestamp_to_datetime(timestamp):
    relevant_timestamp_info = timestamp[:-6]
    formatted_datetime = datetime.strptime(relevant_timestamp_info, '%Y-%m-%dT%H:%M:%S')
    return formatted_datetime



# In[14]:


def convert_pto_to_timestamp(pto_date):
    pto_datetime = datetime.strptime(pto_date, '%Y-%m-%d')
    pto_timestamp = pto_datetime.strftime('%Y-%m-%dT%H:%M:%S')+"+00:00"
    return pto_timestamp


def convert_datetime_to_timestamp(dt):
    timestamp = dt.strftime('%Y-%m-%dT%H:%M:%S')+"+00:00"
    return timestamp

# In[15]:


def create_pto_date_dictionary():
    #set up a dictionary to link project ID to PTO date
    pto_date_dict = {}
    for row in equipment_sheet.rows:
        pto_date = get_equipment_cell_by_column_name(row, "PTO Date").value
        project_id = get_equipment_cell_by_column_name(row, "Project ID").value

        if(type(project_id) == float):
            project_id = str(int(project_id))

        if(project_id != "TEMPLATE" and pto_date is not None):
            pto_timestamp = convert_pto_to_timestamp(pto_date)
            pto_date_dict[project_id] = pto_timestamp
            
    return pto_date_dict


# In[16]:


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

# In[17]:


def find_backup_site_id(backup_project_id):
    backup_site_id = 0
    for site_id, project_id in reference_project_id_map.items():
        if(type(project_id) == list):
            if(str(backup_project_id) in str(project_id)):
                backup_site_id = site_id
        else:
            if(str(project_id) == str(backup_project_id)):
                backup_site_id = site_id

    return backup_site_id


# In[18]:


def generate_oauth_token():
    #Generate API OAuth Token
    url = "https://api.locusenergy.com/oauth/token"

    # payload = "grant_type=password&client_id=5b7bc63684c19388e1d253565cb99980&client_secret=c653512c7190315d6ecf008da9ee3f33&username=safari%20energy&password=iLove$0l@r123#@!"
    payload = "grant_type=password&client_id=5d34704c55b8a82b119c1b96418faada&client_secret=5c628e02730d2d776263880c8b22c3a6&username=pperillo@safarienergy.com&password=Locus123"
    
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
        }

    oauth_response = requests.request("POST", url, data=payload, headers=headers)

    oauth_response_data = json.loads(oauth_response.text)
    
    token = oauth_response_data['access_token']
    
    refresh_token = oauth_response_data['refresh_token']
    
    return token, refresh_token


# In[19]:


def refresh_oauth_token(refresh_token):
    #Refresh API OAuth Token
    url = "https://api.locusenergy.com/oauth/token"

    # payload = "grant_type=refresh_token&client_id=5b7bc63684c19388e1d253565cb99980&client_secret=c653512c7190315d6ecf008da9ee3f33&refresh_token="
    payload = "grant_type=refresh_token&client_id=5d34704c55b8a82b119c1b96418faada&client_secret=5c628e02730d2d776263880c8b22c3a6&refresh_token="
    payload += refresh_token
    
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
        }
    
    oauth_response = requests.request("POST", url, data=payload, headers=headers)
    
    oauth_response_data = json.loads(oauth_response.text)
    
    token = oauth_response_data["access_token"]
    
    return token


# In[20]:


def get_data_available_for_site(site_id):
    #input: site_id (Project's unique Locus ID)
    #output: response data from API call to site data available
    url = "https://api.locusenergy.com/v3/sites/" + str(site_id) + "/dataavailable"

    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': "Bearer " + token,
        'cache-control': "no-cache",
        }

    response = requests.request("GET", url, headers=headers)

    response_data = json.loads(response.text)
    
    return response_data


# In[21]:


def get_data_for_site(site_id, start_date, end_date, short_name):
    #input: site_id, start date, end date, relevant field
    #issue an API call to find the relevant data for a site
    url = "https://api.locusenergy.com/v3/sites/" + str(site_id) + "/data?"
    url += "start=" + format_datetime(start_date)
    url += "&end=" + format_datetime(end_date)
    url += "&tz=UTC&gran=daily"
    url += "&fields=" + short_name

    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': "Bearer " + token,
        'cache-control': "no-cache",
        }

    response = requests.request("GET", url, headers=headers)

    response_data = json.loads(response.text)
    
    return response_data


# In[22]:


def get_site_components(site_id):
    #input: site_id (Project's unique Locus ID)
    #output: response data from API call to site
    url = "https://api.locusenergy.com/v3/sites/" + str(site_id) + "/components"

    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': "Bearer " + token,
        'cache-control': "no-cache",
        }

    response = requests.request("GET", url, headers=headers)

    response_data = json.loads(response.text)
    
    return response_data


def get_site_alerts(site_id):
    #input: site_id (Project's unique Locus ID)
    #output: response data from API call to site
    url = "https://api.locusenergy.com/v3/sites/" + str(site_id) + "/alerts?tz=UTC"

    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': "Bearer " + token,
        'cache-control': "no-cache",
        }

    response = requests.request("GET", url, headers=headers)

    response_data = json.loads(response.text)
    
    return response_data
# In[23]:


def get_data_available_for_component(component_id):
    #input: component_id (component's unique Locus ID)
    #output: response data from API call to component data available
    url = "https://api.locusenergy.com/v3/components/" + str(component_id) + "/dataavailable"

    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': "Bearer " + token,
        'cache-control': "no-cache",
        }

    response = requests.request("GET", url, headers=headers)

    response_data = json.loads(response.text)
    
    return response_data


# In[24]:


def get_data_for_component(component_id, start_date, end_date, short_name):
    #input: component_id, start date, end date, relevant field
    #issue an API call to find the relevant data for a component
    url = "https://api.locusenergy.com/v3/components/" + str(component_id) + "/data?"
    url += "start=" + format_datetime(start_date)
    url += "&end=" + format_datetime(end_date)
    url += "&tz=UTC&gran=daily"
    url += "&fields=" + short_name

    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': "Bearer " + token,
        'cache-control': "no-cache",
        }

    response = requests.request("GET", url, headers=headers)

    response_data = json.loads(response.text)
    
    return response_data


# In[25]:


def find_relevant_fields(component_data):
    #finds the relevant fields of data for a given component
    long_names = []
    short_names = []
    
    for component in component_data["baseFields"]:
        long_name = component["longName"]
        if("aggregations" in list(component.keys())):
            for aggregation in component["aggregations"]:
                short_name = aggregation["shortName"]
                if(short_name.find("Wh_sum") > -1):
                    #print(short_name)
                    short_names.append(short_name)
                    long_names.append(long_name)
                if(short_name.find("Loss") > -1):
                    #print(short_name)
                    short_names.append(short_name)
                    long_names.append(long_name)
                #elif(short_name.find("Wh_e") > -1):
                    #print(short_name)
    
    return short_names, long_names


def pull_component_data(site_id, pto_date):
    #pull list of site components
    components_json = get_site_components(site_id)

    node_types = []
    component_ids = []
    component_names = []
    parent_ids = []
    
    print("Retrieving component data...")
    #collect data about the site's components
    for component in components_json["components"]:
        component_node_type = component["nodeType"]
        node_types.append(component_node_type)
        component_id = str(component['id'])
        component_ids.append(component_id)
        component_name = component["name"]
        component_names.append(component_name)
        parent_id = str(component['parentId'])
        parent_ids.append(parent_id)
        
    #create two dictionaries--component ID:node type and component ID:component name
    component_dict = {a:b for a,b in zip(component_ids, node_types)} 

    component_dict_2 = {a:b for a,b in zip(component_ids, component_names)}

    parent_dict = {a:b for a,b in zip(component_ids, parent_ids)}

    #add components to relevant list if they are of the node type METER
    relevant_components = []

    #add inverters to inverter dict
    inverters, inverter_parents = {},{}

    for item in list(component_dict.keys()):
        if(component_dict[item] == "METER"):
            relevant_components.append(item)
        if(component_dict[item] == "INVERTER"):
            inverters[item] = component_dict_2[item]
            inverter_parents[item] = parent_dict[item]

    relevant_component_names = []

    for component_id in list(component_dict_2.keys()):
        if(component_id in relevant_components):
            relevant_component_names.append(component_dict_2[component_id])

    relevant_node_types = []

    for component_id in list(component_dict.keys()):
        if(component_id in relevant_components):
            relevant_node_types.append(component_dict[component_id])
    
    return component_dict, component_dict_2, relevant_components, inverters, inverter_parents


# In[27]:


def find_relevant_data(relevant_components, component_dict, component_dict_2):
    data_dicts = {}
    all_short_names = []
    #loop over relevant components and retrieve data using API call
    print("Selecting relevant data...")
    for component in relevant_components:
        print("Retrieving data for component ", component_dict_2[component])
        component_data = get_data_available_for_component(component)

        short_names, long_names = find_relevant_fields(component_data)    

        all_short_names.append(short_names)
        
        data_dfs = []

        for i in range(len(short_names)):
            
            #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
            if(pto_date < start_date):
                recent_data = get_data_for_component(component, start_date, end_date, short_names[i])["data"]
                old_data = get_data_for_component(component, pto_date, start_date, short_names[i])["data"]
                
                #merge the two dictionaries
                data = old_data + recent_data
            else:
                #issue an API call for relevant component/field
                data = get_data_for_component(component, start_date, end_date, short_names[i])["data"]
        
            timestamps, datapoints = [],[]
            
            for datapoint in data:
                if(short_names[i] in datapoint.keys()):
                    
                    timestamp = datapoint["ts"]
                    
                    datapoints.append(datapoint[short_names[i]])
                    
                    timestamps.append(timestamp)
        
            #convert from Wh to kWh
            kWh_datapoints = [x / 1000 for x in datapoints]

            data_dict = {short_names_fields_dict[short_names[i]]:kWh_datapoints}
        
            data_df = pd.DataFrame(data_dict)
            data_df.index = timestamps
        
            data_dfs.append(data_df)
    
        component_df = pd.concat(data_dfs, axis=1, sort=False)

        data_dicts[component_dict_2[component]] = component_df

    #remove Ten K Inverter one-off occurence
    if("TenK Inverter" in list(data_dicts.keys())):
        data_dicts.pop("TenK Inverter")
        print("TenK Inverter removed")
    
    #remove consumption meters
    for meter in list(data_dicts.keys()):
        if(meter.find("onsumption") >= 0):
            data_dicts.pop(meter)
            print("Meter: " + meter + " removed")
    
    return data_dicts


# In[28]:

def get_inverter_data(pto_date, inverters):
    inverter_dfs = {}
    short_name = "Wh_sum"
    for inverter_id in inverters:
        inverter_name = inverters[inverter_id]

        #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
        if(pto_date < start_date):
            recent_data = get_data_for_component(inverter_id, start_date, end_date, short_name)["data"]
            old_data = get_data_for_component(inverter_id, pto_date, start_date, short_name)["data"]
            
            #merge the two dictionaries
            data = old_data + recent_data
        else:
            #issue an API call for relevant component/field
            data = get_data_for_component(inverter_id, start_date, end_date, short_name)["data"]

        timestamps, datapoints = [],[]
            
        for datapoint in data:
            if(short_name in datapoint.keys()):
                timestamp = datapoint["ts"]
                
                datapoints.append(datapoint[short_name])
                
                timestamps.append(timestamp)
    
        #convert from Wh to kWh
        kWh_datapoints = [x / 1000 for x in datapoints]

        inverter_dict = {"measured_energy": kWh_datapoints}

        inverter_df = pd.DataFrame(inverter_dict)

        inverter_df.index = timestamps

        inverter_dfs[inverter_id] = inverter_df

    return inverter_dfs

def pull_poa_insolation(component_dict, component_dict_2, pto_date):
    print("Retrieving POA insolation...")
    weatherstations, ws_names = [],[]

    for component in component_dict:
        if(component_dict[component] == "WEATHERSTATION"):
            weatherstations.append(component)
            ws_names.append(component_dict_2[component])
    
    poa_insolation_dfs, ws_details = [], []

    for i in range(len(weatherstations)):
        weatherstation = weatherstations[i]
        ws_name = ws_names[i]

        #check for special case
        if(site_id in FRIT_DIA_other_insolation_ids):
            key = "OTIh_sum"
        else:
            key = "POAIh_sum"

        #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
        if(pto_date < start_date):
            recent_data = get_data_for_component(weatherstation, start_date, end_date, key)
            old_data = get_data_for_component(weatherstation, pto_date, start_date, key)

            #check if the calls were successful
            if("data" in list(recent_data.keys()) and "data" in list(old_data.keys())):
                recent_data = recent_data["data"]
                old_data = old_data["data"]
            else:
                return pd.DataFrame()
            
            #merge the two lists
            poa_insolation_list = old_data + recent_data
            
            #wrap the result in a dictionary
            poa_insolation = {"data": poa_insolation_list}            
        else:
            poa_insolation = get_data_for_component(weatherstation, start_date, end_date, key)
        
        timestamps, datapoints = [],[]

        if("data" in list(poa_insolation.keys())):
            for datapoint in poa_insolation["data"]:
                if(key in list(datapoint.keys())):
                    timestamp = datapoint["ts"]
                                        
                    datapoints.append(datapoint[key]/1000)

                    timestamps.append(timestamp)
                
            data_dict = {"poa_insolation":datapoints}

            data_df = pd.DataFrame(data_dict)
            data_df.index = timestamps
            
            poa_insolation_dfs.append(data_df)
            ws_details.append((weatherstation, ws_name))
    
    if(len(poa_insolation_dfs) == 0):
        print("No POA insolation data available :/")
        poa_insolation_df = pd.DataFrame()
        ws_detail = ""
    
    if(len(poa_insolation_dfs) == 1):
        poa_insolation_df = poa_insolation_dfs[0]
        ws_detail = ws_details[0]
    
    if(len(poa_insolation_dfs) > 1):
        length_list = [len(poa_insolation_df) for poa_insolation_df in poa_insolation_dfs]
        longest_index = length_list.index(max(length_list))
        poa_insolation_df = poa_insolation_dfs[longest_index]
        ws_detail = ws_details[longest_index]
                
    return poa_insolation_df, ws_detail

# In[31]:

def pull_module_temp(component_dict, component_dict_2, pto_date):
    print("Retrieving Module Temperature...")
    weatherstations = []

    for component in component_dict:
        if(component_dict[component] == "WEATHERSTATION"):
            weatherstations.append(component)
    
    module_temp_dfs = []

    for weatherstation in weatherstations:

        key = "TmpBOM_avg"

        #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
        if(pto_date < start_date):
            recent_data = get_data_for_component(weatherstation, start_date, end_date, key)
            old_data = get_data_for_component(weatherstation, pto_date, start_date, key)

            #check if the calls were successful
            if("data" in list(recent_data.keys()) and "data" in list(old_data.keys())):
                recent_data = recent_data["data"]
                old_data = old_data["data"]
            else:
                return pd.DataFrame()
            
            #merge the two lists
            module_temp_list = old_data + recent_data
            
            #wrap the result in a dictionary
            module_temp = {"data": module_temp_list}            
        else:
            module_temp = get_data_for_component(weatherstation, start_date, end_date, key)
        
        timestamps, datapoints = [],[]

        if("data" in list(module_temp.keys())):
            for datapoint in module_temp["data"]:
                if(key in list(datapoint.keys())):
                    timestamp = datapoint["ts"]
                    
                    datapoints.append(datapoint[key])

                    timestamps.append(timestamp)
                
            data_dict = {"module_temp":datapoints}

            data_df = pd.DataFrame(data_dict)
            data_df.index = timestamps
            
            module_temp_dfs.append(data_df)
    
    if(len(module_temp_dfs) == 0):
        print("No Module Temperature data available :/")
        module_temp_df = pd.DataFrame()
    
    if(len(module_temp_dfs) == 1):
        module_temp_df = module_temp_dfs[0]
    
    if(len(module_temp_dfs) > 1):
        length_list = [len(module_temp_df) for module_temp_df in module_temp_dfs]
        longest_index = length_list.index(max(length_list))
        module_temp_df = module_temp_dfs[longest_index]
                
    return module_temp_df


# In[31]:

def pull_ambient_temp(component_dict, component_dict_2, pto_date):
    print("Retrieving Ambient Temperature...")
    weatherstations = []

    for component in component_dict:
        if(component_dict[component] == "WEATHERSTATION"):
            weatherstations.append(component)
    
    ambient_temp_dfs = []

    for weatherstation in weatherstations:

        key = "TmpAmb_avg"

        #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
        if(pto_date < start_date):
            recent_data = get_data_for_component(weatherstation, start_date, end_date, key)
            old_data = get_data_for_component(weatherstation, pto_date, start_date, key)

            #check if the calls were successful
            if("data" in list(recent_data.keys()) and "data" in list(old_data.keys())):
                recent_data = recent_data["data"]
                old_data = old_data["data"]
            else:
                return pd.DataFrame()
            
            #merge the two lists
            ambient_temp_list = old_data + recent_data
            
            #wrap the result in a dictionary
            ambient_temp = {"data": ambient_temp_list}            
        else:
            ambient_temp = get_data_for_component(weatherstation, start_date, end_date, key)
        
        timestamps, datapoints = [],[]

        if("data" in list(ambient_temp.keys())):
            for datapoint in ambient_temp["data"]:
                if(key in list(datapoint.keys())):
                    timestamp = datapoint["ts"]
                    
                    datapoints.append(datapoint[key])

                    timestamps.append(timestamp)
                
            data_dict = {"ambient_temp":datapoints}

            data_df = pd.DataFrame(data_dict)
            data_df.index = timestamps
            
            ambient_temp_dfs.append(data_df)
    
    if(len(ambient_temp_dfs) == 0):
        print("No Ambient Temperature data available :/")
        ambient_temp_df = pd.DataFrame()
    
    if(len(ambient_temp_dfs) == 1):
        ambient_temp_df = ambient_temp_dfs[0]
    
    if(len(ambient_temp_dfs) > 1):
        length_list = [len(ambient_temp_df) for ambient_temp_df in ambient_temp_dfs]
        longest_index = length_list.index(max(length_list))
        ambient_temp_df = ambient_temp_dfs[longest_index]
                
    return ambient_temp_df

def pull_wind_speed(component_dict, component_dict_2, pto_date):
    print("Retrieving Wind Speed...")
    weatherstations = []

    for component in component_dict:
        if(component_dict[component] == "WEATHERSTATION"):
            weatherstations.append(component)
    
    wind_speed_dfs = []

    for weatherstation in weatherstations:

        key = "WndSpd_avg"

        #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
        if(pto_date < start_date):
            recent_data = get_data_for_component(weatherstation, start_date, end_date, key)
            old_data = get_data_for_component(weatherstation, pto_date, start_date, key)

            #check if the calls were successful
            if("data" in list(recent_data.keys()) and "data" in list(old_data.keys())):
                recent_data = recent_data["data"]
                old_data = old_data["data"]
            else:
                return pd.DataFrame()
            
            #merge the two lists
            wind_speed_list = old_data + recent_data
            
            #wrap the result in a dictionary
            wind_speed = {"data": wind_speed_list}            
        else:
            wind_speed = get_data_for_component(weatherstation, start_date, end_date, key)
        
        timestamps, datapoints = [],[]

        if("data" in list(wind_speed.keys())):
            for datapoint in wind_speed["data"]:
                if(key in list(datapoint.keys())):
                    timestamp = datapoint["ts"]
                    
                    datapoints.append(datapoint[key])

                    timestamps.append(timestamp)
                
            data_dict = {"wind_speed":datapoints}

            data_df = pd.DataFrame(data_dict)
            data_df.index = timestamps
            
            wind_speed_dfs.append(data_df)
    
    if(len(wind_speed_dfs) == 0):
        print("No Wind Speed data available :/")
        wind_speed_df = pd.DataFrame()
    
    if(len(wind_speed_dfs) == 1):
        wind_speed_df = wind_speed_dfs[0]
    
    if(len(wind_speed_dfs) > 1):
        length_list = [len(wind_speed_df) for wind_speed_df in wind_speed_dfs]
        longest_index = length_list.index(max(length_list))
        wind_speed_df = wind_speed_dfs[longest_index]
                
    return wind_speed_df

def pull_backup_poa_insolation(component_dict, component_dict_2, pto_date, backup_site_id):
    print("Retrieving POA insolation...")
    weatherstations = []
    
    for component in component_dict:
        if(component_dict[component] == "WEATHERSTATION"):
            weatherstations.append(component)
    
    poa_insolation_dfs, ws_details = [], []

    for weatherstation in weatherstations:

        #check for special case
        if(backup_site_id in FRIT_DIA_other_insolation_ids):
            key = "OTIh_sum"
        else:
            key = "POAIh_sum"

        #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
        if(pto_date < start_date):
            recent_data = get_data_for_component(weatherstation, start_date, end_date, key)
            old_data = get_data_for_component(weatherstation, pto_date, start_date, key)

            #check if the calls were successful
            if("data" in list(recent_data.keys()) and "data" in list(old_data.keys())):
                recent_data = recent_data["data"]
                old_data = old_data["data"]
            else:
                return pd.DataFrame()
            
            #merge the two lists
            poa_insolation_list = old_data + recent_data
            
            #wrap the result in a dictionary
            poa_insolation = {"data": poa_insolation_list}            
        else:
            poa_insolation = get_data_for_component(weatherstation, start_date, end_date, key)
        
        timestamps, datapoints = [],[],

        if("data" in list(poa_insolation.keys())):
            for datapoint in poa_insolation["data"]:
                if(key in list(datapoint.keys())):
                    timestamp = datapoint["ts"]
                    
                    datapoints.append(datapoint[key]/1000)

                    timestamps.append(timestamp)
                
            data_dict = {"backup_poa_insolation":datapoints}

            data_df = pd.DataFrame(data_dict)
            data_df.index = timestamps
            
            ws_name = component_dict_2[weatherstation]

            poa_insolation_dfs.append(data_df)
            ws_details.append((weatherstation, ws_name))
    
    if(len(poa_insolation_dfs) == 0):
        print("No Backup POA insolation data available :/")
        poa_insolation_df = pd.DataFrame()
        ws_detail = ("", "")
    
    if(len(poa_insolation_dfs) == 1):
        poa_insolation_df = poa_insolation_dfs[0]
        ws_detail = ws_details[0]
    
    if(len(poa_insolation_dfs) > 1):
        length_list = [len(poa_insolation_df) for poa_insolation_df in poa_insolation_dfs]
        longest_index = length_list.index(max(length_list))
        poa_insolation_df = poa_insolation_dfs[longest_index]
        ws_detail = ws_details[longest_index]
                
    return poa_insolation_df, ws_detail

def pull_expected_insolation(pto_date):
    print("Retrieving expected insolation...")
    
    #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
    if(pto_date < start_date):
        recent_data = get_data_for_site(site_id, start_date, end_date, "Ih_e_sum")["data"]
        old_data = get_data_for_site(site_id, pto_date, start_date, "Ih_e_sum")["data"]
        
        #merge the two lists
        expected_insolation_list = old_data + recent_data

        #wrap the result in a dictionary
        expected_insolation = {"data": expected_insolation_list}
    else:
        expected_insolation = get_data_for_site(site_id, start_date, end_date, "Ih_e_sum")
    
    expected_insolations, timestamps = [],[]
    expected_insolation_df = pd.DataFrame()
    #store expected energy data in list then DataFrame
    if("data" in list(expected_insolation.keys())):
        for datapoint in expected_insolation["data"]:
            if("Ih_e_sum" in datapoint.keys()):
                insolation = datapoint["Ih_e_sum"]/1000
                timestamp = datapoint["ts"]

                expected_insolations.append(insolation)
                timestamps.append(timestamp)
                
    if(len(expected_insolations) > 0):
        expected_insolation_dict = {"expected_insolation": expected_insolations}
        
        expected_insolation_df = pd.DataFrame(expected_insolation_dict)
        
        expected_insolation_df.index = timestamps

    return expected_insolation_df

def pull_backup_expected_insolation(pto_date, backup_site_id):
    print("Retrieving backup expected insolation...")
    
    #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
    if(pto_date < start_date):
        recent_data = get_data_for_site(backup_site_id, start_date, end_date, "Ih_e_sum")["data"]
        old_data = get_data_for_site(backup_site_id, pto_date, start_date, "Ih_e_sum")["data"]
        
        #merge the two lists
        expected_insolation_list = old_data + recent_data

        #wrap the result in a dictionary
        expected_insolation = {"data": expected_insolation_list}
    else:
        expected_insolation = get_data_for_site(backup_site_id, start_date, end_date, "Ih_e_sum")
    
    expected_insolations, timestamps = [],[]
    expected_insolation_df = pd.DataFrame()
    #store expected energy data in list then DataFrame
    if("data" in list(expected_insolation.keys())):
        for datapoint in expected_insolation["data"]:
            if("Ih_e_sum" in datapoint.keys()):
                insolation = datapoint["Ih_e_sum"]/1000
                timestamp = datapoint["ts"]

                expected_insolations.append(insolation)
                timestamps.append(timestamp)
                    
    if(len(expected_insolations) > 0):
        expected_insolation_dict = {"backup_expected_insolation": expected_insolations}
        
        expected_insolation_df = pd.DataFrame(expected_insolation_dict)
        
        expected_insolation_df.index = timestamps

    return expected_insolation_df

def pull_expected_energy(pto_date):
    print("Retrieving expected energy...")
    
    #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
    if(pto_date < start_date):
        recent_data = get_data_for_site(site_id, start_date, end_date, "Wh_e_sum")["data"]
        old_data = get_data_for_site(site_id, pto_date, start_date, "Wh_e_sum")["data"]
        
        #merge the two lists
        expected_energy_list = old_data + recent_data

        #wrap the result in a dictionary
        expected_energy = {"data": expected_energy_list}
    else:
        expected_energy = get_data_for_site(site_id, start_date, end_date, "Wh_e_sum")
    
    expected_energies, timestamps = [],[]
    expected_energy_df = pd.DataFrame()
    #store expected energy data in list then DataFrame
    if("data" in list(expected_energy.keys())):
        for datapoint in expected_energy["data"]:
            if("Wh_e_sum" in datapoint.keys()):
                energy = datapoint["Wh_e_sum"]/1000
                timestamp = datapoint["ts"]

                expected_energies.append(energy)
                timestamps.append(timestamp)
                
    if(len(expected_energies) > 0):
        expected_energy_dict = {"expected_energy": expected_energies}
        
        expected_energy_df = pd.DataFrame(expected_energy_dict)
        
        expected_energy_df.index = timestamps

    return expected_energy_df


# In[43]:
def pull_max_snow_depth(pto_date):
    print("Retrieving Max Snow Depth...")
    
    #check if pto_date is before start_date. If so, make two API calls to encompass 10 years
    if(pto_date < start_date):
        recent_data = get_data_for_site(site_id, start_date, end_date, "Snow_m_max")["data"]
        old_data = get_data_for_site(site_id, pto_date, start_date, "Snow_m_max")["data"]
        
        #merge the two lists
        max_snow_depth_list = old_data + recent_data

        #wrap the result in a dictionary
        max_snow_depth = {"data": max_snow_depth_list}
    else:
        max_snow_depth = get_data_for_site(site_id, start_date, end_date, "Snow_m_max")
    
    max_snow_depths, timestamps = [],[]
    max_snow_depth_df = pd.DataFrame()
    #store Max Snow Depth data in list then DataFrame
    if("data" in list(max_snow_depth.keys())):
        for datapoint in max_snow_depth["data"]:
            if("Snow_m_max" in datapoint.keys()):
                max_snow_depth = datapoint["Snow_m_max"]
                timestamp = datapoint["ts"]

                max_snow_depths.append(max_snow_depth)
                timestamps.append(timestamp)
    
    if(len(max_snow_depths) > 0):
        max_snow_depth_dict = {"max_snow_depth": max_snow_depths}
        
        max_snow_depth_df = pd.DataFrame(max_snow_depth_dict)
        
        max_snow_depth_df.index = timestamps

    return max_snow_depth_df

def pull_and_insert_alerts(timestamps):
    print("Retrieving and processing Alerts...")
    #retrieve all alerts from Locus
    response = get_site_alerts(site_id)

    open_alerts_list, alert_descriptions_list = [],[]
    #loop over all days in format_df
    for timestamp in timestamps:
        
        #convert each timestamp to a date
        dt = convert_timestamp_to_datetime(timestamp)
        row_date = date(dt.year, dt.month, dt.day)
    
        open_alerts = 0
        alert_descriptions = []
        #loop over all alerts to find which are open on this date
        for alert in response['alerts']:
            #retrieve metadata from alert
            alert_type = alert['alertType']
            start = alert['start']
            try:
                end = alert['end']
                #convert alert end date to datetime
                end_dt = convert_timestamp_to_datetime(end)
                end_date = date(end_dt.year, end_dt.month, end_dt.day)
            except:
                now = datetime.now()
                end_date = date(now.year, now.month, now.day)

            #convert alert start date to datetime
            start_dt = convert_timestamp_to_datetime(start)
            start_date = date(start_dt.year, start_dt.month, start_dt.day)

            if(start_date <= row_date):
                if(end_date >= row_date):
                    open_alerts += 1
                    alert_descriptions.append(alert_type)
        
        alert_string = format_alert_string(alert_descriptions)
        
        open_alerts_list.append(open_alerts)
        alert_descriptions_list.append(alert_string)

    alerts_df = pd.DataFrame({"open_alerts": open_alerts_list, "alert_descriptions": alert_descriptions_list})
    alerts_df.index = timestamps

    return alerts_df

def format_alert_string(alert_descriptions):
    alert_string = ""
    
    if(len(alert_descriptions) == 0):
        return alert_string
    
    alerts = list(set(alert_descriptions))
    for i in range(len(alerts)):
        alert = alerts[i]
        if(i == len(alerts) - 1):
            alert_string += alert
        else:
            alert_string += (alert + " - ")
    
    return alert_string


def find_first_and_final_timestamp(data_dicts, project_id):
    #find the first and last timestamps in any data call
    print("Finding datetime range...")
    first_timestamps, final_timestamps = [],[]
    for component in data_dicts:
        data_dfs = data_dicts[component]
        for data_df in data_dfs:
            first_timestamp = list(data_df.index)[0]
            final_timestamp = list(data_df.index)[-1]
            first_timestamps.append(first_timestamp)
            final_timestamps.append(final_timestamp)

    first_timestamp = min(first_timestamps)
    final_timestamp = max(final_timestamps)
    
    #if PTO date is in the equipment library, use maximum of two options as first timestamp
    if(project_id in list(pto_date_dict.keys())):
        first_timestamp = convert_timestamp_to_datetime(pto_date_dict[project_id])
        first_timestamp = format_timestamp(first_timestamp)
    
    return first_timestamp, final_timestamp


def find_timestamp_range(pto_date):
    #find all timestamps for this project's lifetime
    first_datetime = datetime(pto_date.year, pto_date.month, pto_date.day, 0,0,0)

    current_datetime = datetime.now()
    
    final_datetime = datetime(current_datetime.year, current_datetime.month, current_datetime.day, 0,0,0)

    number_of_days = (final_datetime - first_datetime).days

    all_timestamps, all_datetimes = [],[]
    for i in range(number_of_days):
        dt = first_datetime + timedelta(days=i)
        timestamp = convert_datetime_to_timestamp(dt)
        all_timestamps.append(timestamp)
        all_datetimes.append(dt)
    
    return all_timestamps, all_datetimes


def construct_index_df(first_timestamp, final_timestamp, short_names, project_id, site_id):
    #construct a dataframe with the appropriate format to hold all locus data
    print("Constructing formatted DataFrame...")
    
    #find the first and last datetime
    first_datetime = datetime.strptime(first_timestamp[:-3], '%Y-%m-%dT%H:%M:%S+%f')
    final_datetime = datetime.strptime(final_timestamp[:-3], '%Y-%m-%dT%H:%M:%S+%f')
    day_difference = final_datetime - first_datetime

    #create a list of dates, years, months, days from the first to the last
    date_list = [(first_datetime + timedelta(days=i)) for i in range(day_difference.days+1)]

    timestamp_list = [time.strftime('%Y-%m-%dT%H:%M:%S')+"+00:00" for time in date_list]
    year_list = [time.year for time in date_list]
    month_list = [time.month for time in date_list]
    day_list = [time.day for time in date_list]
    
    month_name_list = [month_names[month-1] for month in month_list]
    
    length = len(timestamp_list)
    
    #create lists for project id and site id and pro forma
    project_id_list = [project_id]*length
    site_id_list = [site_id]*length
    pro_forma_list = [0.0]*length
    poa_insolation_list = [0.0]*length
    expected_insolation_list = [0.0]*length
    expected_energy_list = [0.0]*length
    backup_poa_insolation_list = [0.0]*length
    backup_expected_insolation_list = [0.0]*length
    module_temp_list = [0.0]*length
    ambient_temp_list = [0.0]*length
    wind_speed_list = [0.0]*length
    max_snow_depth_list = [0.0]*length
    open_alerts_list = [0]*length
    alerts_list = [""]*length
    
    #create a dictionary to create a dataframe from
    index_dict = {"Timestamp": timestamp_list, "Project ID": project_id_list, "Site ID": site_id_list,
                  "Year": year_list, "Month": month_list, "Month Name": month_name_list, "Day": day_list,
                  "Pro Forma": pro_forma_list, "expected_energy": expected_energy_list,
                  "poa_insolation": poa_insolation_list, "expected_insolation": expected_insolation_list,
                  "backup_poa_insolation": backup_poa_insolation_list,
                  "backup_expected_insolation": backup_expected_insolation_list,
                  "module_temp": module_temp_list, "ambient_temp": ambient_temp_list,
                  "wind_speed": wind_speed_list, "max_snow_depth": max_snow_depth_list,
                  "open_alerts": open_alerts_list, "alert_descriptions": alerts_list}

    format_df = pd.DataFrame(index_dict)
    
    #create columns for each field
    short_name_dict = {}
    for short_name in short_names:
        if("Loss" in short_name):
            short_name_list = [np.nan]*len(format_df)
        else:
            short_name_list = [0.0]*len(format_df)
        short_name_dict[short_name] = short_name_list
        
    short_name_df = pd.DataFrame(short_name_dict)

    #concatenate all dfs into one
    format_df = pd.concat([format_df, short_name_df], axis=1, sort=False)

    #reindex the df with timestamp
    format_df.index = format_df["Timestamp"]
    
    return format_df


# In[45]:


#PRO FORMA


# In[46]:


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

    pro_forma_df.columns = ["Project ID", "Project Name", "Month", "Month Name", "Year", "Day", "pro_forma"]
    
    months = list(pro_forma_df["Month"])
    years = list(pro_forma_df["Year"])
    days = list(pro_forma_df["Day"])

    timestamps = [datetime(int(year), int(month), int(day)).strftime('%Y-%m-%dT%H:%M:%S')+"+00:00"
                for year, month, day in zip(years, months, days)]

    pro_forma_df.index = timestamps

    pro_forma_df = pro_forma_df.drop(["Project Name", "Month", "Month Name", "Year", "Day", "Project ID"], axis=1)

    return pro_forma_df

def extract_project_pro_forma_rows(project_id, pro_forma_df):
    #extract relevant pro forma df rows
    project_ids = list(pro_forma_df["Project ID"])
    project_index = project_ids.index(project_id)
    degradation = pro_forma_df.at[project_index, "Degradation"]
    pro_formas, years, months = [],[],[]
    for i in range(project_index, project_index+12):
        pro_forma = pro_forma_df.at[i, "Pro Forma"]
        year = pro_forma_df.at[i, "Year"]
        month = pro_forma_df.at[i, "Month"]

        months.append(month)
        years.append(year)
        pro_formas.append(pro_forma)

    pro_forma_dict = {"Year": years, "Month": months, "Pro Forma": pro_formas}
    project_pro_forma_df = pd.DataFrame(pro_forma_dict)
    
    project_pro_forma_df.index = months
    
    return project_pro_forma_df, degradation

def find_pto_data(project_pro_forma_df):
    #find pto data from project pro forma df
    pto_year = int(project_pro_forma_df.at[list(project_pro_forma_df.index)[0], "Year"])
    pto_month_name = project_pro_forma_df.at[list(project_pro_forma_df.index)[0], "Month"]
    pto_month = month_names.index(pto_month_name) + 1
    
    return pto_year, pto_month, pto_month_name

def extrapolate_pro_forma_datapoint(month, year, pto_month, pto_year, pro_forma, degradation):
    #extrapolate a single pro forma datapoint
    degradation_factor = 1-degradation
    
    if(month < pto_month and year <= pto_year):
        #Prior to PTO Date, return 0
        return 0
    if(year < pto_year):
        #Prior to PTO Date, return 0
        return 0
    
    if(month >= pto_month):
        degradation_power = year - pto_year
        if(degradation_power == 0):
            return pro_forma/month_lengths[month]
        else:
            return (pro_forma * (degradation_factor**degradation_power))/month_lengths[month]
    
    elif(month < pto_month):
        degradation_power = year - pto_year - 1
        return (pro_forma * (degradation_factor**degradation_power))/month_lengths[month]

def extrapolate_all_project_pro_forma(format_df, project_pro_forma_df, pto_month, pto_year, degradation):
    #calculate and save all pro forma for a project
    for timestamp in list(format_df.index):
        
        month_name = format_df.at[timestamp, "Month Name"]
        month = format_df.at[timestamp, "Month"]
        year = format_df.at[timestamp, "Year"]
        
        pro_forma = project_pro_forma_df.at[month_name, "Pro Forma"]
        
        new_pro_forma = extrapolate_pro_forma_datapoint(month, year, pto_month, pto_year, pro_forma, degradation)
        
        format_df.at[timestamp, "Pro Forma"] = new_pro_forma
    
    return format_df

def pull_and_extrapolate_pro_forma(format_df, pro_forma_df, project_id):
    #execute three successive functions to pull and calculate project pro forma
    print("Collecting and extrapolating Pro Forma data...")
    
    if(str(project_id) in list(pro_forma_df["Project ID"])):
        project_pro_forma_df, degradation = extract_project_pro_forma_rows(project_id, pro_forma_df)

        pto_year, pto_month, pto_month_name = find_pto_data(project_pro_forma_df)

        new_pro_forma_df = extrapolate_all_project_pro_forma(format_df, project_pro_forma_df, pto_month, pto_year, degradation)
    
        return new_pro_forma_df
    else:
        return format_df

def aggregate_data(data_dicts, timestamps):
    #fill up the formatted df with data from data_dicts
    print("Aggregating data...")
    
    data_dfs = list(data_dicts.values())
    final_df = data_dfs[0]

    df_timestamps = list(final_df.index)
    extra_timestamps = [item for item in df_timestamps if item not in timestamps]
    for i in range(1, len(data_dfs)):
        data_df = data_dfs[i]
        final_df = final_df.add(data_df, fill_value = 0)

        df_timestamps = list(data_df.index)
        df_extra_timestamps = [item for item in df_timestamps if item not in timestamps]
        extra_timestamps = extra_timestamps + df_extra_timestamps

        for component in data_dicts:
            if(component == "Losses Reporting"):
                #loss adjustment factor
                adjustment_factor = 1

    extra_timestamps = list(set(extra_timestamps))

    #remove the extra rows coming from stray datapoints
    try:
        final_df = final_df.drop(extra_timestamps, axis=0, errors='ignore')
        print("Extra Timestamps deleted...")
    except:
        print("Extra  Timestamps included...")

    return final_df

def format_final_dataframe(final_df, df_list, project_id, site_id, timestamps):
    
    years, months, month_names_list, days = [],[],[],[]
    for timestamp in timestamps:
        dt = convert_timestamp_to_datetime(timestamp)

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

    final_df = pd.concat([format_df, final_df], axis=1, sort=False)

    extra_timestamps = []
    for df in df_list:
        df_timestamps = list(df.index)
        final_df = pd.concat([final_df, df], axis=1, sort=False)
        df_extra_timestamps = [item for item in df_timestamps if item not in timestamps]
        extra_timestamps = extra_timestamps + df_extra_timestamps

    extra_timestamps = list(set(extra_timestamps))

    #remove the extra rows coming from Pro Forma DB
    try:
        final_df = final_df.drop(extra_timestamps, axis=0, errors='ignore')
        print("Extra Timestamps deleted...")
    except:
        print("Extra  Timestamps included...")

    return final_df


###
#DATABASE
###

def create_database(database_path):
    #Create database engine
    engine = create_engine(database_path, echo = False)
    meta = MetaData()

    # #Drop table if it already exists
    # sql = text('DROP TABLE IF EXISTS energy;')
    # result = engine.execute(sql)

    # #create energy table
    # energy = Table(
    #     'energy', meta,
    #     Column('project_id', String),
    #     Column('site_id', String),
    #     Column('month_name', String),
    #     Column('month', String),
    #     Column('year', String),
    #     Column('day', String),
    #     Column('pro_forma', Float),
    #     Column('measured_energy', Float),
    #     Column('expected_energy', Float),
    #     Column('poa_insolation', Float),
    #     Column('expected_insolation', Float),
    #     Column('backup_poa_insolation', Float),
    #     Column('backup_expected_insolation', Float),        
    #     Column('clipping', Float),
    #     Column('degradation', Float),
    #     Column('downtime', Float),
    #     Column('other', Float),
    #     Column('partial_downtime', Float),
    #     Column('shading', Float),
    #     Column('snow', Float),
    #     Column('soiling', Float),
    #     Column('module_temp', Float),
    #     Column('ambient_temp', Float),
    #     Column('wind_speed', Float),
    #     Column('max_snow_depth', Float),
    #     Column('open_alerts', Float),
    #     Column('alert_descriptions', String),
    # )

    meta.create_all(engine)

    #connect to engine
    conn = engine.connect()
    
    return conn
    # return energy, conn


# In[55]:


def insert_into_energy_database(final_df, energy, conn):
    #insert data into energy table
    print("Saving data to database...")
    for i in final_df.index:
        year = final_df.at[i, "Year"]
        month = final_df.at[i, "Month"]
        day = final_df.at[i, "Day"]
        month_name = final_df.at[i, "Month Name"]
        project_id = final_df.at[i, "Project ID"]
        site_id = final_df.at[i, "Site ID"]
        pro_forma = final_df.at[i, "Pro Forma"]
        measured_energy = final_df.at[i, "measured_energy"]
        expected_energy = final_df.at[i, "expected_energy"]
        poa_insolation = final_df.at[i, "poa_insolation"]
        expected_insolation = final_df.at[i, "expected_insolation"]
        backup_poa_insolation = final_df.at[i, "backup_poa_insolation"]
        backup_expected_insolation = final_df.at[i, "backup_expected_insolation"]       
        clipping = final_df.at[i, "Clipping Loss"]
        degradation = final_df.at[i, "Degradation Loss"]
        downtime = final_df.at[i, "Downtime Loss"]
        other = final_df.at[i, "Other Loss"]
        partial_downtime = final_df.at[i, "Partial Downtime Loss"]
        shading = final_df.at[i, "Shading Loss"]
        snow = final_df.at[i, "Snow Loss"]
        soiling = final_df.at[i, "Soiling Loss"]        
        module_temp = final_df.at[i, "module_temp"]
        ambient_temp = final_df.at[i, "ambient_temp"]
        wind_speed = final_df.at[i, "wind_speed"]
        max_snow_depth = final_df.at[i, "max_snow_depth"]
        #open_alerts = final_df.at[i, "open_alerts"]
        #alert_descriptions = final_df.at[i, "alert_descriptions"]
        
        s = energy.insert().values(year = str(year), month = str(month), day = str(day), month_name = month_name,
                                   project_id = str(project_id), site_id = str(site_id), pro_forma = pro_forma,
                                   measured_energy = measured_energy, expected_energy = expected_energy,
                                   poa_insolation = poa_insolation, expected_insolation = expected_insolation,
                                   backup_poa_insolation = backup_poa_insolation,
                                   backup_expected_insolation = backup_expected_insolation,
                                   clipping = clipping, degradation = degradation, downtime = downtime,
                                   other = other, partial_downtime = partial_downtime, shading = shading,
                                   snow = snow, soiling = soiling, module_temp = module_temp,
                                   ambient_temp = ambient_temp, wind_speed = wind_speed,
                                   max_snow_depth = max_snow_depth) #, open_alerts = open_alerts,
                                   #alert_descriptions = alert_descriptions)

        conn.execute(s)

def calculate_and_insert(relevant_components, inverters, component_dict, component_dict_2, site_id, project_id, pto_date,
                         backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date):
    #Aggregate function to run all of the functions in question
    
    #refresh access token
    token = refresh_oauth_token(refresh_token)
    
    #find the relevant data within the component data
    data_dicts = find_relevant_data(relevant_components, component_dict, component_dict_2)

    #find the beginning and end times appearing in Locus data
    all_timestamps, all_datetimes = find_timestamp_range(pto_date)

    #construct the format of the final df
    #format_df = construct_index_df(first_timestamp, final_timestamp, all_short_names, project_id, site_id)
    
    #pull pro forma from Pro Forma database
    pro_forma_df = pull_pro_forma(project_id)

    #pull POA insolation data from Locus and insert it into format_df
    poa_insolation_df, ws_detail = pull_poa_insolation(component_dict, component_dict_2, pto_date)
    print("Selecting Weather Station...")
    print("Weather station for project", project_id, ":", ws_detail)

    #pull Module Temperature data from Locus and insert it into format_df
    module_temp_df = pull_module_temp(component_dict, component_dict_2, pto_date)

    #pull Ambient Temperature data from Locus and insert it into format_df
    ambient_temp_df = pull_ambient_temp(component_dict, component_dict_2, pto_date)

    #pull Wind Speed data from Locus and insert it into format_df
    wind_speed_df = pull_wind_speed(component_dict, component_dict_2, pto_date)

    #pull Expected insolation data from Locus and insert it into format_df
    expected_insolation_df = pull_expected_insolation(pto_date)

    #pull Expected energy data from Locus and insert it into format_df
    expected_energy_df = pull_expected_energy(pto_date)

    #pull Max Snow Depth from Locus and insert it into format_df
    max_snow_depth_df = pull_max_snow_depth(pto_date)

    #pull backup Expected insolation and data from Locus and insert it into format_df
    backup_expected_insolation_df = pull_backup_expected_insolation(pto_date, backup_site_id)

    #pull POA insolation data from Locus and insert it into format_df
    backup_poa_insolation_df, backup_ws_detail = pull_backup_poa_insolation(backup_component_dict,
                                                    backup_component_dict_2, backup_pto_date, backup_site_id)
    print("Backup weather station for project", project_id, ":", backup_ws_detail)

    #pull Alerts from Locus and insert them into format_df
    #alerts_df = pull_and_insert_alerts(all_timestamps)

    #store all DataFrames as a list for concatenation
    df_list = [pro_forma_df, poa_insolation_df, module_temp_df, ambient_temp_df, wind_speed_df,
               expected_insolation_df, expected_energy_df, max_snow_depth_df,
               backup_expected_insolation_df, backup_poa_insolation_df]

    #fill up the formatted df with data from locus calls
    final_df = aggregate_data(data_dicts, all_timestamps)

    #pull inverter level production data
    #inverter_dfs = get_inverter_data(pto_date, inverters)

    #save the final_df to a dictionary for debugging
    #final_df_dict[project_id] = final_df

    #format final dataframe
    final_df = format_final_dataframe(final_df, df_list, project_id, site_id, all_timestamps)

    # now = datetime.now()
    # now = now.strftime("%Y-%m-%d_%H-%M-%S")
    # final_df.to_csv(f"C:\\Users\\NadimAtalla\\Desktop\\temp\\final_df_data_{now}.csv")

    #send the data to the energy and losses databases
    #insert_into_energy_database(final_df, energy, conn)

    print("Saving to database...")
    global database_df
    database_df = database_df.append(final_df, ignore_index=True)
    # final_df.to_sql("energy", conn, if_exists='append', index=False)
    
    '''
    #aggregate meter level data
    for meter in data_dicts:
        #find the meter id using meter name
        for meter_node_id, meter_name in component_dict_2.items():
            if(meter_name == meter):
                meter_id = meter_node_id
        
        print("Saving meter ", meter, " to meter level database...")
        data_df = data_dicts[meter]
        meter_df = format_meter_dataframe(meter, meter_id, data_df, final_df, project_id, site_id, all_timestamps)
        
        meter_df.to_sql("meter_level", meter_conn, if_exists='append', index=False)
    
    #aggregate inverter level data
    for inverter_id in inverter_dfs:
        inverter_name = inverters[inverter_id]
        inverter_df = inverter_dfs[inverter_id]
        
        print("Saving inverter ", inverter_name, " to inverter level database...")
        final_inverter_df = format_inverter_dataframe(inverter_name, inverter_id, inverter_df, final_df, project_id, site_id, all_timestamps)
        
        final_inverter_df.to_sql("inverter_level", inverter_conn, if_exists='append', index=False)

        #insert data for meter into database
        #insert_into_meter_database(meter, meter_id, meter_df, meter_level, meter_conn)
    '''


def format_meter_dataframe(meter, meter_id, data_df, final_df, project_id, site_id, timestamps):
    #create lists for datetime entry
    years, months, month_names_list, days = [],[],[],[]
    for timestamp in timestamps:
        dt = convert_timestamp_to_datetime(timestamp)

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
    meter_names = [meter]*len(timestamps)
    meter_ids = [meter_id]*len(timestamps)
    
    #create the standard formatted section of meter_df
    format_df = pd.DataFrame({"project_id": project_ids, "site_id": site_ids, "meter_name": meter_names,
                              "meter_id": meter_ids, "year": years, "month": months, "day": days,
                              "month_name": month_names_list})    
    format_df.index = timestamps

    #list the columns that will be added from final_df
    columns_to_add = ['pro_forma', 'expected_energy', 'poa_insolation', 'expected_insolation',
                      'backup_poa_insolation', 'backup_expected_insolation']
    
    #for each column, check if it exists in final_df. If not, fill with NaN
    dfs_to_add = []
    for column in columns_to_add:
        if(column in final_df):
            column_df = final_df[column]
        else:
            column_df = pd.DataFrame({column: [None]*len(timestamps)})
    
        column_df.index = timestamps
        dfs_to_add.append(column_df)
    
    #merge all the dfs from final_df
    site_meter_df = pd.concat(dfs_to_add, axis=1, sort=False)
    
    meter_df = pd.concat([format_df, data_df, site_meter_df], axis=1, sort=False)

    df_timestamps = list(meter_df.index)
    extra_timestamps = [item for item in df_timestamps if item not in timestamps]

    #remove the extra rows coming from Pro Forma DB
    try:
        meter_df = meter_df.drop(extra_timestamps, axis=0, errors='ignore')
        print("Extra Timestamps deleted...")
    except:
        print("Extra  Timestamps included...")

    return meter_df

def format_inverter_dataframe(inverter, inverter_id, data_df, final_df, project_id, site_id, timestamps):
    #create lists for datetime entry
    years, months, month_names_list, days = [],[],[],[]
    for timestamp in timestamps:
        dt = convert_timestamp_to_datetime(timestamp)

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
    inverter_names = [inverter]*len(timestamps)
    inverter_ids = [inverter_id]*len(timestamps)
    
    #create the standard formatted section of inverter_df
    format_df = pd.DataFrame({"project_id": project_ids, "site_id": site_ids, "inverter_name": inverter_names,
                              "inverter_id": inverter_ids, "year": years, "month": months, "day": days,
                              "month_name": month_names_list})
    format_df.index = timestamps

    #list the columns that will be added from final_df
    columns_to_add = ['pro_forma', 'expected_energy', 'poa_insolation', 'expected_insolation',
                      'backup_poa_insolation', 'backup_expected_insolation']
    
    #for each column, check if it exists in final_df. If not, fill with NaN
    dfs_to_add = []
    for column in columns_to_add:
        if(column in final_df):
            column_df = final_df[column]
        else:
            print("Null column")
            column_df = pd.DataFrame({column: [None]*len(timestamps)})
    
        column_df.index = timestamps
        dfs_to_add.append(column_df)
    
    #merge all the dfs from final_df
    site_inverter_df = pd.concat(dfs_to_add, axis=1, sort=False)
    
    inverter_df = pd.concat([format_df, data_df, site_inverter_df], axis=1, sort=False)

    df_timestamps = list(inverter_df.index)
    extra_timestamps = [item for item in df_timestamps if item not in timestamps]

    #remove the extra rows coming from Pro Forma DB
    try:
        inverter_df = inverter_df.drop(extra_timestamps, axis=0, errors='ignore')
        print("Extra Timestamps deleted...")
    except:
        print("Extra  Timestamps included...")

    return inverter_df
        
def aggregate_meter_data(meter, data_dict, format_df):
    #fill up the formatted df with data from data_dicts
    print("Aggregating data for meter " + meter + "...")
    
    #prepare meter_df
    meter_df = format_df.copy()
    for short_name in all_short_names:
        if(short_name in meter_df.columns):
            meter_df = meter_df.drop(short_name, axis=1)
    
    #iterate over the meter level data and save as separate dataframes
    
    for data_df in data_dict:
        short_name = data_df.columns[-1]
        
        #create a new column DataFrame for each field
        column_df = pd.DataFrame([0.0]*len(meter_df))
        column_df.index = meter_df.index
        column_df.columns = [short_name]
        
        #fill in the column with appropriate data
        for row in column_df.index:
            if(row in data_df.index):
                column_df.at[row, short_name] = data_df.at[row, short_name]/1000
        
        meter_df = pd.concat([meter_df, column_df], axis=1, sort=False)
    
    
    #ensure that meter_df has all the columns, fill in blank data with NaN
    for short_name in all_short_names:
        if(short_name not in meter_df.columns):
            column_df = pd.DataFrame([np.nan] * len(meter_df))
            column_df.index = meter_df.index
            column_df.columns = [short_name]
            
            meter_df = pd.concat([meter_df, column_df], axis=1, sort=False)
    
    return meter_df

def create_meter_database(meter_database_path):
    #Create database engine
    engine = create_engine(meter_database_path, echo = False)
    meta = MetaData()

    #Drop table if it already exists
#     sql = text('DROP TABLE IF EXISTS meter_level;')
#     result = engine.execute(sql)

#     #create energy table
#     meter_level = Table(
#         'meter_level', meta,
#         Column('project_id', String),
#         Column('site_id', String),
#         Column('meter_name', String),
#         Column('meter_id', String),
#         Column('month_name', String),
#         Column('month', String),
#         Column('year', String),
#         Column('day', String),
#         Column('pro_forma', Float),
#         Column('measured_energy', Float),
#         Column('expected_energy', Float),
#         Column('poa_insolation', Float),
#         Column('expected_insolation', Float),
#         Column('backup_poa_insolation', Float),
#         Column('backup_expected_insolation', Float),        
#         Column('clipping', Float),
#         Column('degradation', Float),
#         Column('downtime', Float),
#         Column('other', Float),
#         Column('partial_downtime', Float),
#         Column('shading', Float),
#         Column('snow', Float),
#         Column('soiling', Float)
#     )

#     meta.create_all(engine)

#     #connect to engine
#     meter_conn = engine.connect()
    
#     return meter_level, meter_conn

# def create_inverter_database(inverter_database_path):
#     #Create database engine
#     engine = create_engine(inverter_database_path, echo = False)
#     meta = MetaData()

#     #Drop table if it already exists
#     sql = text('DROP TABLE IF EXISTS inverter_level;')
#     result = engine.execute(sql)

#     #create energy table
#     inverter_level = Table(
#         'inverter_level', meta,
#         Column('project_id', String),
#         Column('site_id', String),
#         Column('inverter_name', String),
#         Column('inverter_id', String),
#         Column('month_name', String),
#         Column('month', String),
#         Column('year', String),
#         Column('day', String),
#         Column('pro_forma', Float),
#         Column('measured_energy', Float),
#         Column('expected_energy', Float),
#         Column('poa_insolation', Float),
#         Column('expected_insolation', Float),
#         Column('backup_poa_insolation', Float),
#         Column('backup_expected_insolation', Float)
#     )

    meta.create_all(engine)

    #connect to engine
    inverter_conn = engine.connect()
    
    return inverter_level, inverter_conn

def insert_into_meter_database(meter, meter_id, meter_df, meter_level, meter_conn):
    #insert data into meter table
    print("Saving meter " + meter + " data to database...")
    for i in meter_df.index:
        year = meter_df.at[i, "Year"]
        month = meter_df.at[i, "Month"]
        day = meter_df.at[i, "Day"]
        month_name = meter_df.at[i, "Month Name"]
        project_id = meter_df.at[i, "Project ID"]
        site_id = meter_df.at[i, "Site ID"]
        pro_forma = meter_df.at[i, "Pro Forma"]
        measured_energy = meter_df.at[i, "Wh_sum"]
        expected_energy = meter_df.at[i, "expected_energy"]
        poa_insolation = meter_df.at[i, "poa_insolation"]
        expected_insolation = meter_df.at[i, "expected_insolation"]
        backup_poa_insolation = meter_df.at[i, "backup_poa_insolation"]
        backup_expected_insolation = meter_df.at[i, "backup_expected_insolation"]       
        clipping = meter_df.at[i, "Wh_estClippingLoss_sum"]
        degradation = meter_df.at[i, "Wh_estDegradLoss_sum"]
        downtime = meter_df.at[i, "Wh_estDowntimeLoss_sum"]
        other = meter_df.at[i, "Wh_estOtherLoss_sum"]
        partial_downtime = meter_df.at[i, "Wh_estPartialDowntimeLoss_sum"]
        shading = meter_df.at[i, "Wh_estShadingLoss_sum"]
        snow = meter_df.at[i, "Wh_estSnowLoss_sum"]
        soiling = meter_df.at[i, "Wh_estSoilingLoss_sum"]
        meter_name = meter
        meter_id = meter_id

        s = meter_level.insert().values(year = str(year), month = str(month), day = str(day), month_name = month_name,
                                   project_id = str(project_id), site_id = str(site_id), pro_forma = pro_forma,
                                   measured_energy = measured_energy, expected_energy = expected_energy,
                                   poa_insolation = poa_insolation, expected_insolation = expected_insolation,
                                   backup_poa_insolation = backup_poa_insolation,
                                   backup_expected_insolation = backup_expected_insolation,
                                   clipping = clipping, degradation = degradation, downtime = downtime,
                                   other = other, partial_downtime = partial_downtime, shading = shading,
                                   snow = snow, soiling = soiling, meter_name = meter_name, meter_id = meter_id)

        meter_conn.execute(s)

#SPECIAL CASES

def deal_with_special_site(project_id, pto_date, backup_site_id, backup_pto_date):
    print("Working on: " + str(project_id))
    #generate a new OAuth token
    #token = generate_oauth_token()

    #pull all data about each component in current site
    component_dict, component_dict_2, relevant_components, inverters, inverter_parents = pull_component_data(site_id, pto_date)
    
    #pull relevant components from a predefined dictionary linking appropriate meters
    relevant_components = special_projects_component_dict[project_id]
    
    #pull all data about each component in backup site
    backup_component_dict, backup_component_dict_2, _, _, _ = pull_component_data(backup_site_id, backup_pto_date)
    
    #Define special site inverters
    print("Selecting child inverters...")
    special_site_inverters = {}
    for inverter in inverters:
        if(inverter_parents[inverter] in relevant_components):
            special_site_inverters[inverter] = inverters[inverter]

    #run the site through the aggregated calculation function
    calculate_and_insert(relevant_components, special_site_inverters, component_dict, component_dict_2, site_id, project_id,
                         pto_date, backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)


def deal_with_unique_site(site_id_argument, project_id_argument, pto_date, backup_site_id, backup_pto_date):
    #pull all data about each component in backup site
    backup_component_dict, backup_component_dict_2, _, _, _ = pull_component_data(backup_site_id, backup_pto_date)
    
    if(site_id == 3621460):
        print("Working on Riverhead")
        deal_with_riverhead(site_id_argument, project_id_argument, pto_date,
                            backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)
    if(site_id == 3689266):
        print("Working on The Point")
        deal_with_the_point([3689266, 3689289, 3689279], project_id_argument, pto_date,
                            backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)
    if(site_id == 3588745):
        print("Working on Stonestown")
        deal_with_stonestown(site_id_argument, project_id_argument, pto_date,
                             backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)


def deal_with_riverhead(site_id, unique_project_ids, pto_date,
                        backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date):
    #pull component data for both site IDs on Locus
    component_dict, component_dict_2, relevant_components, inverters, inverter_parents = pull_component_data(3621460, pto_date)
    component_dict_prime, component_dict_2_prime, relevant_components_prime, inverters_prime, inverter_parents_prime = pull_component_data(3604572, pto_date)
    
    #Aggregate the data from the
    relevant_components = relevant_components + relevant_components_prime
    inverters.update(inverters_prime)
    inverter_parents.update(inverter_parents_prime)
    component_dict.update(component_dict_prime)
    component_dict_2.update(component_dict_2_prime)
    
    nem_components, fit_components = [],[]
    
    for component in relevant_components:
        component_name = component_dict_2[component]
        if("NEM" in component_name):
            nem_components.append(component)
        if("FIT" in component_name):
            fit_components.append(component)
    
    print("NEM", nem_components)
    print("FIT", fit_components)
    
    #Define special site inverters
    print("Selecting child inverters...")
    fit_inverters, nem_inverters = {},{}
    for inverter in inverters:
        if(inverter_parents[inverter] in fit_components):
            fit_inverters[inverter] = inverters[inverter]
        if(inverter_parents[inverter] in fit_components):
            nem_inverters[inverter] = inverters[inverter]

    #set FIT and NEM PTO Dates
    fit_pto_date = datetime(2017, 2, 17)
    nem_pto_date = datetime(2016, 8, 2)
    
    print("FIT components")
    print(fit_components)
    print(fit_inverters)
    print("NEM components")
    print(nem_components)
    print(fit_components)

    #Run all relevant functions on both projects
    #FIT
    calculate_and_insert(fit_components, fit_inverters, component_dict, component_dict_2, site_id, str(unique_project_ids[0]),
                         fit_pto_date, backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)
    #NEM
    calculate_and_insert(nem_components, nem_inverters, component_dict, component_dict_2, site_id, str(unique_project_ids[1]),
                         nem_pto_date, backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)


def deal_with_the_point(site_ids, project_id, pto_date,
                        backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date):
    #pull component data for all three site IDs on Locus
    all_relevant_components = []
    all_component_dicts, all_component_dicts_2, all_inverters = {},{},{}
    for site_id in site_ids:
        component_dict, component_dict_2, relevant_components, inverters, inverter_parents = pull_component_data(site_id, pto_date)
        all_relevant_components = all_relevant_components + relevant_components
        all_inverters.update(inverters)
        all_component_dicts.update(component_dict)
        all_component_dicts_2.update(component_dict_2)

    print("The Point Components")
    print(all_relevant_components)
    print(all_inverters)

    calculate_and_insert(all_relevant_components, all_inverters, all_component_dicts, all_component_dicts_2, site_id, str(project_id),
                         pto_date, backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)


def deal_with_stonestown(site_id, project_id, pto_date,
                         backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date):
    component_dict, component_dict_2, relevant_components, inverters, inverter_parents = pull_component_data(site_id, pto_date)
    
    deck_inverters, roof_inverters = [],[]
    deck_inverters_dict, roof_inverters_dict = {},{}
    for component in list(component_dict.keys()):
        component_type = component_dict[component]
        component_name = component_dict_2[component]
        if(component_type == "INVERTER"):
            if("Deck" in component_name):
                deck_inverters.append(component)
                deck_inverters_dict[component] = component_name
            else:
                roof_inverters.append(component)
                roof_inverters_dict[component] = component_name
    
    print("Roof inverters")
    print(roof_inverters)
    print(roof_inverters_dict)
    print("Deck inverters")
    print(deck_inverters)
    print(deck_inverters_dict)

    roof_pto_date = datetime(2016, 8, 26)
    calculate_and_insert(roof_inverters, roof_inverters_dict, component_dict, component_dict_2, site_id, str(614), roof_pto_date,
                         backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)
    token = refresh_oauth_token(refresh_token)
    deck_pto_date = datetime(2017, 6, 22)
    calculate_and_insert(deck_inverters, deck_inverters_dict, component_dict, component_dict_2, site_id, str(442), deck_pto_date,
                         backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)

###
#CODE BODY
###

#setup logging filename
# log_name = log_filename()
log_name = debug_log_filename() #UNCOMMENT

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


def send_error_message(site_id):
    body = 'Subject: Pegasus Error\n\n' + 'Error encountered while running project: ' + str(site_id) + " :("
    try:
        smtpObj = smtplib.SMTP('smtp-mail.outlook.com', 587)
    except Exception as e:
        print(e)
        smtpObj = smtplib.SMTP_SSL('smtp-mail.outlook.com', 465)
    #type(smtpObj) 
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login('natalla@safarienergy.com', "998thLCoftheNW") 
    smtpObj.sendmail('natalla@safarienergy.com', 'natalla@safarienergy.com', body)
    smtpObj.sendmail('natalla@safarienergy.com', 'pperillo@safarienergy.com', body)

    smtpObj.quit()

'''
class Tee:
    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)
    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2

sys.stdout = Tee(open(log_name, "w"), sys.stdout)
'''

month_lengths = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

all_short_names = ["Wh_sum", "Wh_estDowntimeLoss_sum", "Wh_estClippingLoss_sum", "Wh_estSnowLoss_sum",
                   "Wh_estDegradLoss_sum", "Wh_estSoilingLoss_sum", "Wh_estShadingLoss_sum",
                   "Wh_estOtherLoss_sum", "Wh_estPartialDowntimeLoss_sum"]

short_names_fields_dict = {"Wh_sum": "measured_energy", "Wh_estDowntimeLoss_sum": "downtime",
                           "Wh_estClippingLoss_sum": "clipping", "Wh_estSnowLoss_sum": "snow",
                           "Wh_estDegradLoss_sum": "degradation", "Wh_estSoilingLoss_sum": "soiling",
                           "Wh_estShadingLoss_sum": "shading", "Wh_estOtherLoss_sum": "other",
                           "Wh_estPartialDowntimeLoss_sum": "partial_downtime"}

special_site_ids = [3433389, 3433394, 3582909, 3433076, 3433393, 3535900, 3506659, 3511757, 3433388, 3433392, 3568351]

unique_site_ids = [3621460, 3689266, 3588745]

special_projects_component_dict = {"613":['2294219', '2294231'], "191a": ['2294165', '2294181'], 
                                   "191b": ['2543203', '2543210', '2543207', '2543373'], #Northridge
                                   "1979": ['2112206'], "1980": ['2518032'], "1981": ['2112205'], #Queen Anne
                                   "1962": ['2112124'], "1963": ['2112125'], "1964": ['2112126'],
                                   "1975": ['2112127', '2112128'], #Ellisburg
                                   "1967": ['2107235'], "1968": ['2107237'], "1969": ['2107233'],
                                   "1970": ['2107234'], "1971": ['2107236'], "1976": ['2107238'], #Mercer
                                   "1972": ['2107662'], "214": ['2107661'], "217": ['2107663'], #Troy
                                   "1973": ['2238317', '2238318'],
                                   "1978": ['2238319', '2238320', '2238321', '2238322', '2238323'], #Bridgewater
                                   "1958": ['2203958'], "1959": ['2203959'], "1960": ['2203957'], "1961": ['2203960'], #Saw Mill
                                   "1952": ['2210955'], "1953": ['2211362'], "1954": ['2210958'],
                                   "1955": ['2210957'], "1956": ['2210956'], "1957": ['2211360'], #N Bedford
                                   "1965": ['2107642'], "1966": ['2107641'],  #Brick Plaza
                                   "1982": ['2107657'], "1983": ['2107656'], #Bristol Plaza
                                   "341": ['2281996', '2282014'], "1002": ['2511283', '2511431'] #Buckland Hills
                                  }

FRIT_DIA_other_insolation_ids = [3433388, 3433389, 3433076, 3433393, 3511757, 3506659, 3512254, 3506570]

start_date, end_date = find_previous_day()

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


token, refresh_token = generate_oauth_token()


#issues an api call for all Safari Energy projects
# url = "https://api.locusenergy.com/v3/partners/411744/sites"
url = "https://api.locusenergy.com/v3/partners/747035/sites"

headers = {
    'Content-Type': "application/x-www-form-urlencoded",
    'Authorization': "Bearer " + token,
    'cache-control': "no-cache",
    }

sites = requests.request("GET", url, headers=headers)

site_data = json.loads(sites.text)

#prints the names and IDs of all Safari Energy projects
site_names = []
site_ids = []

for site in site_data["sites"]:
    site_name = site["name"]
    site_id = int(site["id"])
    print(site_name, site_id)
    site_names.append(site_name)
    site_ids.append(site_id)

#set a map of project IDs from Locus to Safari convention
#AZ
project_id_map_az = {3571994:247, 3557194:"432a", 3846630:2844}

#CA
project_id_map_ca = {3799063:2590, 3793478:2592, 3829691:2274, 
                     3747023:612, 3697553:436, 3595990:193, 3582909:[613, "191a", "191b"], 3846627:2845,
                     3629486:441, 3588753:616, 3544322:443, 3819457:2573, 3588745:[614, 442],
                     3833902:2223, 3830272:2328, 3830859:2539, 3835190:2225, 3829631:2224, 3829702:2269,
                     3829639:2359, 3837068:2277, 3829690:2344}

#CO
project_id_map_co = {3589242:439, 3665932:435, 3803796:1513, 3803808:1926}

#CT
project_id_map_ct = {3781114:987, 3768839:1077, 3508078:381, 3508096:306, 3678856:1003, 3686788:1004, 3823540:1876,
                     3431410:161, 3431411:163, 3431412:162, 3740362:1630, 3784471:2776, 3767662:1019, 3560866:342,
                     3462701:164, 3818455:901, 3568351: [341, 1002]}

#DE
project_id_map_de = {3544389:425, 3669861:"541b"}

#FL
project_id_map_fl = {3745054:1160, 3744325:1161, 3762679:1170, 3739746:510, 3793008:2775, 3792471:1162,
                     3792399:2783, 3586452:449, 3694081:448, 3589532:450, 3815042:471, 3793126:472, 3822507:2784}

#GA
project_id_map_ga = {3515163:445}

#HI
project_id_map_hi = {3667744:"123b", 3659133:"123c", 3537264:106, 3714091:1071, 3667745:"123a",
                     3792329:1038, 3512306:124, 3508359:99}

#MA
project_id_map_ma = {3421704:1989, 3420025:1987, 3665933:1008, 3421701:199, 3771951:1631, 3420306:1986,
                     3740364:1727, 3725685:1632, 3783068:170, 
                     3589543:"122b", 3590858:"122a", 3771496:1227, 3631079:219, 3799441:1923,
                     3824689:3934, 3827309:3927, 3829311:3923, 3831335:3925,
                     3829314:3920, 3835497:3921, 3829630:3924, 3829635:3926, 3834349:4811, 3834347:4809, 
                     3833892:4808, 3833893:4807, 3833894:4806, 3833895:4804, 3833896:4805, 3822483:1037,
                     3829689:3928, 3834350:4812, 3850339:4810, 3829312:3931, 3831336:4815}

#MD/DC
project_id_map_dc = {3671359:1005, 3745582:1184, 3745581:1767, 3760394:1766,
                     3544396:427, 3592000:428, 3544401:431, 3544399:429,
                     3672319:467, 3689285:470, 3795270:1181, 3799457:1186, 3787248:1188}

#NJ
project_id_map_nj = {3768837:224, 3689287:1024, 3689284:1027, 3431416:206, 3431414:1988, 3689203:1023, 3689198:1025,
                     3689196:1020, 3431417:204, 3431415:205, 3689283:1028, 3777966:1029, 3750701:1728, 3798951:2943,
                     3783069:1919, 3535900: [1973, 1978],
                     3542992:1974, 3649867:590, 3541375:1621, 3679287:589, 3537108:3027, 3770517:1722, 3691108:973,
                     3690621:64, 3688803:174, 3586530:242, 3666139:320, 3778932:85, 3793128:1918, 3821984:3021, 3821985:891,
                     3774315:1354}

#NV
project_id_map_nv = {3585644:446, 3589395:444}

#NY
project_id_map_ny = {3511757:1952, 3666293:273, 3512254:141, 3506570:140, 3509492:347,
                     3418857:194, 3678371:1010, 3431382:133, 3418856:127, 3416800:131, 3799436:1921,
                     3416799:132, 3420026:128, 3420030:130, 
                     3588374:121, 3595400:104, 3431413:129,
                     3506659: [1958, 1959, 1960, 1961], 3511757: [1952, 1953, 1954, 1955, 1956, 1957],
                     3379748:110, 3379747:108, 3383315:111, 3381843:113, 3383324:115, 3382576:117,
                     3382570:119, 3792330:2945, 3621460:[1410,105], 3822695: 1039}

#OH
project_id_map_oh = {3689205:1067}

#OR
project_id_map_or = {3785778:1195}

#PA
project_id_map_pa = {3799593:2418, 3793127:2557, 3799445:2417, 3848881:2414}

#RI
project_id_map_ri = {3689264:1030, 3689288:1031}

#SC
project_id_map_sc = {3536028:433}

#TX
project_id_map_tx = {3669903:1015, 3681172:1018, 3670065:1016, 3669904:1014, 3681170:1017, 3754524:1726, 3745579:1725}


#Adjustment factors for missing losses data
adjustment_factor_map = {3433394:1.94, 3626442:2.5, 3433389:3.65, 3433388:1.513, 3747023:6.6,
                         3588753:10.29, 3611429:1.76, 3462701:5.54, 3745054:3.49, 3744325:3, 3626442:2.5,
                         3592000:1.91, 3544399:2.44, 3585644:1.28, 3689205:2, 3689266:1.8384}

project_id_maps = [project_id_map_az, project_id_map_ca, project_id_map_co, project_id_map_ct,
                   project_id_map_de, project_id_map_fl, project_id_map_ga, project_id_map_hi,
                   project_id_map_ma, project_id_map_dc, project_id_map_nj, project_id_map_ny,
                   project_id_map_nv, project_id_map_oh, project_id_map_or, project_id_map_pa,
                   project_id_map_ri, project_id_map_sc, project_id_map_tx]



# In[75]:


# In[76]:

#create the project ID map from Equipment Library
project_id_map, _ = create_project_id_maps()

#create a reference_project_id_map
reference_project_id_map = project_id_map.copy()

frit_project_id_map = {3433392: [1982, 1983], #Bristol
                       3433394:[1979, 1980, 1981], #Queen Anne
                       3626582:628, #NY Huntington Shopping Center
                       3433391:179} #NY Huntington Square

database_path = 'sqlite:///C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus Beta 1.0.db'
conn = create_database(database_path)

#meter_database_path = 'sqlite:///C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus Meter Level 1.0.db'
#meter_level, meter_conn = create_meter_database(meter_database_path)

#inverter_database_path = 'sqlite:///C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus Inverter Level 1.0.db'
#inverter_level, inverter_conn = create_inverter_database(inverter_database_path)

pro_forma_table, pro_forma_conn = connect_to_pro_forma_db()

final_df_dict = {}
i = 0
counter = 0

print(project_id_map)
raise Exception("shfdjvfdjvdhvhfdjkfdnvfd") #delete

#loop over all sites in the map
for site_id in list(project_id_map.keys()):
    continue #delete
    counter+=1
    print(f"{counter}/{len(list(project_id_map.keys()))}")
    
    try:
        start_time = datetime.now()
        #refresh OAuth token every 20 sites
        i += 1
        token = refresh_oauth_token(refresh_token) 

        #Check for special case sites
        if(site_id in special_site_ids):
            for i in range(len(project_id_map[site_id])):
                project_id = str(project_id_map[site_id][i])
                backup_project_id = backup_insolation_dict[project_id]
                backup_site_id = find_backup_site_id(backup_project_id)
                pto_date = convert_timestamp_to_datetime(pto_date_dict[project_id])
                backup_pto_date = convert_timestamp_to_datetime(pto_date_dict[backup_project_id])
                deal_with_special_site(project_id, pto_date, backup_site_id, backup_pto_date)

        #Check for unique sites
        elif(site_id in unique_site_ids):
            unique_project_ids = project_id_map[site_id]

            #find the PTO date (first check for how many projects exist)
            if(type(unique_project_ids) == list):
                pto_date = convert_timestamp_to_datetime(pto_date_dict[str(unique_project_ids[0])])
                backup_project_id = backup_insolation_dict[str(unique_project_ids[0])]
            else:
                pto_date = convert_timestamp_to_datetime(pto_date_dict[str(unique_project_ids)])
                backup_project_id = backup_insolation_dict[str(unique_project_ids)]

            backup_site_id = find_backup_site_id(backup_project_id)
            backup_pto_date = convert_timestamp_to_datetime(pto_date_dict[backup_project_id])

            deal_with_unique_site(site_id, unique_project_ids, pto_date, backup_site_id, backup_pto_date)

        else:
            #set the project id from the predefined project_id_map
            project_id = str(project_id_map[site_id])

            #find the PTO date for 5+ years check
            pto_date = convert_timestamp_to_datetime(pto_date_dict[project_id])

            #find the backup site ID for insolation
            backup_project_id = backup_insolation_dict[project_id]
            backup_site_id = find_backup_site_id(backup_project_id)
            backup_pto_date = convert_timestamp_to_datetime(pto_date_dict[backup_project_id])

            print("Working on: " + str(project_id))
            #pull all data about each component in current site
            component_dict, component_dict_2, relevant_components, inverters, inverter_parents = pull_component_data(site_id, pto_date)

            #pull all data about each component in backup site
            backup_component_dict, backup_component_dict_2, _, _, _ = pull_component_data(backup_site_id, backup_pto_date)

            calculate_and_insert(relevant_components, inverters, component_dict, component_dict_2, site_id, str(project_id), pto_date,
                                backup_site_id, backup_component_dict, backup_component_dict_2, backup_pto_date)

            print("Data saved!")

        #calculate time taken for each site
        end_time = datetime.now()
        time_taken = (end_time - start_time).seconds
        print("Site runtime: ", time_taken, " seconds")

    except Exception as e:
        print(e)
        print("Error encountered")
        #logger.exception("Error encountered")
        #send_error_message(site_id)

# In[83]:

#close the database connections
conn.close()
#meter_conn.close()
pro_forma_conn.close()


# In[84]:


#final_df_dict[list(final_df_dict.keys())[-1]]

now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M-%S")
database_df.to_csv(f"C:\\Users\\NadimAtalla\\Desktop\\temp\\database_df_{now}.csv")

# In[85]:


print("Saving DB Backup...")
source = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus Beta 1.0.db"

destination = datetime_filename()

# shutil.copy(source, destination)


print("Saving Meter Level DB Backup...")
source = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus Meter Level 1.0.db"

destination = meter_datetime_filename()

#shutil.copy(source, destination)

print("Saving Inverter Level DB Backup...")
source = "C:/Users/NadimAtalla/Safari Energy, LLC/Paulo Perillo - Pegasus/Pegasus Inverter Level 1.0.db"

destination = inverter_datetime_filename()

#shutil.copy(source, destination)


# In[87]:

#calculate time taken for entire script
script_end_time = datetime.now()
time_taken = (script_end_time - script_start_time).seconds
print("Script runtime: ", time_taken, " seconds")

print("Script computation complete!")
# %%

# %%
