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
