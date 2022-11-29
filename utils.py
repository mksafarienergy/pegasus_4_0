"""
timestamps (ts_list_split_list) is a list of lists. The inner lists holds timestamps incrementing by 1 day 
    starting from the start date. The inner list holds 1500 dates and then the list continues on the 
    next element. TODO: This might be able to be replaced by a function that just finds the date 
    1500 days later instead of holding a list that creates all the timestamps for it. 
"""


from datetime import datetime
from datetime import timedelta



def get_time_now() -> str:
    now = datetime.now()
    return now.strftime("%Y-%m-%d_%H-%M-%S")


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

    some_date_obj = datetime.strptime(str(some_date), '%Y-%m-%dT%H:%M:%S') + timedelta(days=number_of_days)
    return some_date_obj.strftime('%Y-%m-%dT%H:%M:%S')


def convert_datetime_to_timestamp(some_date: datetime) -> str:
    """
    convert datetime object to the string format accepted by Locus api

    :param some_date: some date
    :return: string date of format %Y-%m-%dT%H:%M:%S+00:00 (type:str)
    """
    timestamp = some_date.strftime('%Y-%m-%dT%H:%M:%S') + "+00:00"
    return timestamp


def timestamp_splitter(timestamps: list, interval: int) -> list[list]:
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


def get_timestamps(start_date: datetime) -> list[list[str]]:
    """
    Might deprecate this. See notes at top of this file.
    """

    first_datetime = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
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
    ts_list_split_list = timestamp_splitter(ts_mod, 1500)

    print(len(ts_list_split_list))
    print(len(ts_list_split_list[0]))

    return ts_list_split_list

