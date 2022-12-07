"""
timestamps (ts_list_split_list) is a list of lists. The inner lists holds timestamps incrementing by 1 day 
    starting from the start date. The inner list holds 1500 dates and then the list continues on the 
    next element. TODO: This might be able to be replaced by a function that just finds the date 
    1500 days later instead of holding a list that creates all the timestamps for it. 

TODO: Figure out when the best time to put the +00:00 at the end of a timestamp
    At the time of writing this it seems that the Locus Api can use a timestamp
    without the +00:00 and the response will include it. So, it might be useful
    to keep all timestamps without the +00:00 and when formatting the data
    we can include the +00:00 back.
"""


from datetime import datetime
from datetime import timedelta
from dateutil import rrule

import pandas as pd


def get_time_now() -> str:
    now = datetime.now()
    return now.strftime("%Y-%m-%d_%H-%M-%S")


def get_todays_timestamp() -> str:
    today = datetime.now()
    today = datetime(today.year, today.month, today.day, 0, 0, 0)
    return convert_datetime_to_timestamp(today)


def reformat_date_to_timestamp(date_string: str) -> str:
    """
    Reformatting date from using / to -

    :param date_string: date of format %Y-%m-%d
    :return: date of format %Y-%m-%dT%H:%M:%S
    """
    
    return datetime.strptime(date_string, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S')


def add_days_to_date(some_date: str, number_of_days: int) -> str:
    """
    Adding number of days to the given date. 
        One use is that api calls retrieve 
        data from start_date to end_date (excluding 
        end_date). Adding one extra day will
        retrieve the exact days that are mentioned

    :param some_date: some date
    :param number_of_days: some number of days to be 
                            added to some date
    :return: some date plus number of days 
    """

    some_datetime = datetime.strptime(str(some_date), '%Y-%m-%dT%H:%M:%S') + timedelta(days=number_of_days)
    return some_datetime.strftime('%Y-%m-%dT%H:%M:%S')


def convert_datetime_to_timestamp(some_date: datetime) -> str:
    """
    convert datetime object to the string format accepted by Locus api

    :param some_date: some date
    :return: string date of format %Y-%m-%dT%H:%M:%S+00:00
    """

    timestamp = some_date.strftime('%Y-%m-%dT%H:%M:%S') + "+00:00"
    return timestamp


def convert_timestamp_to_datetime(timestamp: str) -> datetime:
    """
    Convert string formatted date to a datetime object

    :param timestamp: date of format %Y-%m-%dT%H:%M:%S (type: str)
    :return: datetime object
    """

    if '+' in timestamp:
        timestamp = timestamp.split('+')[0]
    return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')

    
def split_timestamps_into_intervals(timestamps: list, interval: int) -> list[list]:
    """
    splitting list of timestamps at regular intervals
        ex: len(timestamps) is 3100 elements long
            inverval is 1500
            return [[first 1500 timestamps], [next 1500 timestamps], [last 100]]

    :param timestamps: list of timestamps from start to end dates (incrementing by 1)
    :param interval: desired regular interval of timestamps considered for api call
    :return: list[lists] of timestamps splitted at an interval
    """

    new_list = list(map(list, zip(*([iter(timestamps)] * interval))))
    new_len = len(new_list) * interval
    org_len = len(timestamps)
    diff = org_len - new_len
    new_list.append(timestamps[-diff:])
    
    return new_list


def get_timestamps(start_date: str) -> tuple[list[list[str]], list]:
    """
    Might deprecate this. See notes at top of this file.
    """

    start_date_obj = convert_timestamp_to_datetime(start_date)
    first_datetime = datetime(start_date_obj.year, start_date_obj.month, start_date_obj.day, 0, 0, 0)
    current_datetime = datetime.now()
    final_datetime = datetime(current_datetime.year, current_datetime.month, current_datetime.day, 0, 0, 0)
    number_of_days = (final_datetime - first_datetime).days

    all_timestamps, all_datetimes = [], []
    for i in range(number_of_days):
        date = first_datetime + timedelta(days=i)
        timestamp = convert_datetime_to_timestamp(date)
        all_timestamps.append(timestamp)
        all_datetimes.append(date)

    ts_mod = [time.split('+')[0] for time in all_timestamps]
    ts_list_split_list = split_timestamps_into_intervals(ts_mod, 1500)

    print(len(ts_list_split_list))
    print(len(ts_list_split_list[0]))

    return ts_list_split_list, all_timestamps


def create_timestamp_list(start_date: str, end_date: str) -> list[str]:
    """Creating a list of timestamps"""

    end_date_obj = convert_timestamp_to_datetime(end_date)
    start_date_obj = convert_timestamp_to_datetime(start_date)
    dates = rrule.rrule(rrule.DAILY, dtstart=start_date_obj, until=end_date_obj)
    return [d.strftime('%Y-%m-%dT%H:%M:%S') + '+00:00' for d in dates]


def get_cell_by_column_name_from_asset_id(df: pd.DataFrame, asset_id: str, column_name: str):
    """
    Getting a specific value from a df based on the given asset id

    :param df: dataframe from which the desired value is being held in
    :param asset_id: id of the row we want to get the information from
    :param column_name: the column name we want to get the information for
    :return: the value we got from the df
    """

    print('get_cell_by_column_name_from_asset_id')
    print(df.loc[df['asset_id'] == asset_id, str(column_name)])
    return df.loc[df['asset_id'] == asset_id, str(column_name)].iloc[0]
