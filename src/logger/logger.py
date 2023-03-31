from datetime import datetime
import logging
import sys


def log_filename() -> str:
    """
    Generates the file path string for the new log file every time the 
        script starts
    """
    
    date = datetime.now()
    date_time_string = date.strftime("%Y-%m-%d %H-%M-%S")
    
    return f"./logs/Log - {date_time_string}.log"


def set_up_logger(log_path=log_filename()) -> None:
    """
    Sets up the logger. All stdout statements will go to the logger
    """
    
    # Creating new log path string
    log_path = log_filename()

    # set up a logger to log messages and errors
    logger = logging.getLogger()
    logging.basicConfig(filename = log_path, level = logging.INFO)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    fh = logging.FileHandler(filename=log_path)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # log all printed text to log file
    log = open(log_path, "a")
    sys.stdout = log


# def log(message) -> None:
#     """
#     Logs message in certain format.
#     First 19 spaces are for time, and the rest is the message itself
#     """
#     try:
#         time_now = datetime.now()
#         date_time_string = time_now.strftime("%Y-%m-%d %H-%M-%S")

#         print(f'{date_time_string:<19} | {message}')
#         # print("{:<19} | {}".format(date_time_string, message))
#     except Exception as e:
#         log('Error in ogger: log')
#         log(e)

def log(message) -> None:
    """
    Logs message in certain format.
    First 19 spaces are for time, and the rest is the message itself
    """
    try:
        time_now = datetime.now()
        date_time_string = time_now.strftime("%Y-%m-%d %H-%M-%S")

        # Convert non-string objects to strings
        if not isinstance(message, str):
            message = str(message)

        # Replace Ohm symbol (Ω) with 'O'
        message = message.replace('\u2126', 'O')

        print(f'{date_time_string:<19} | {message}')
    except Exception as e:
        log('Error in logger: log')
        log(str(e))