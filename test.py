from datetime import datetime
from datetime import timedelta
import time


# t_list = ['2022-11-16T00:00:00', '2022-11-17T00:00:00', '2022-11-18T00:00:00']

# # Creating an iterator with 3 values in the list
# #  And having the list be 1500 elements long. Each
# #  of those elements are one iterator of 3 values
# i = zip(*([iter(t_list)] * 1500))
# new_list = list(map(list, zip(*([iter(t_list)] * 1500))))
# print(new_list)
# # i = [iter(t_list)] * 5
# i = zip(*([iter(t_list)] * 5))
# print(list(i))
# print(len(i))

# m = map(list, i)
# print(m)
# # print(len(m))

# l = list(m)
# print(l)
# print(next(i[0]))
# print(next(i[1]))
# print(i[0])
# print(i[1])
# print(i[2])


# i = zip(*([iter(t_list)] * 1500))
# new_list = list(map(list, i))


# new_list = list(map(list, zip(*([iter(original_list)] * split_interval))))
# new_len = len(new_list) * split_interval
# org_len = len(original_list)
# diff = org_len - new_len
# new_list.append(original_list[-diff:])
# return new_list



def find_timestamp_range(start_date):
    """
    create timestamps from start date till end date

    :param start_date: datetime obj (type: obj)

    :return: timestamps (type:list), timestamp objects (type:list)
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
    
    return all_timestamps, all_datetimes


def convert_datetime_to_timestamp(date):
    """
    convert datetime object to string of format as in the timestamp of api

    :param date: datetime obj (type: obj)

    :return: date of format %Y-%m-%dT%H:%M:%S+00:00 (type:str)
    """
    timestamp = date.strftime('%Y-%m-%dT%H:%M:%S') + "+00:00"
    return timestamp


def convert_timestamp_to_datetime(timestamp):
    """
    to change a date of format %Y-%m-%dT%H:%M:%S to date object

    :param timestamp: date of format %Y-%m-%dT%H:%M:%S (type: str)

    :return: date object (type: datetime.obj)
    """
    formatted_datetime = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
    return formatted_datetime


def timestamp_splitter(original_list, split_interval):
    """
    splitting list of timestamps at the split interval

    :param original_list: timestamps from start to end dates (type: list)
    :param split_interval: desired regular interval of timestamps
                           considered for api call (type: int)

    :return: list of lists of timestamps splitted at a regular interval
             with the last list with the remaining timestamps(type: list)
    """
    new_list = list(map(list, zip(*([iter(original_list)] * split_interval))))
    new_len = len(new_list) * split_interval
    org_len = len(original_list)
    diff = org_len - new_len
    new_list.append(original_list[-diff:])
    return new_list


if __name__ == '__main__':
    print('start')
    # start_date = '2013-11-11T00:00:00'
    # start_date = convert_timestamp_to_datetime(start_date)
    # all_timestamps, all_datetimes = find_timestamp_range(start_date)
    # ts_mod = [time.split('+')[0] for time in all_timestamps]
    # ts_list_split_list = timestamp_splitter(ts_mod, 1500)
    # # print(ts_list_split_list)
    # print(len(ts_list_split_list))
    # for e in ts_list_split_list:
    #     if len(e) < 1500:
    #         print(e)
    #         print(len(e))


    current_datetime = datetime.now()
    final_datetime = datetime(current_datetime.year, current_datetime.month, current_datetime.day, 0, 0, 0)

    date = final_datetime + timedelta(days=-1500)
    print(final_datetime)
    print(date)
    # date = date - timedelta(days=1500)
    # print(date)
    print('end')
