from datetime import datetime

def get_time_now() -> datetime:
    now = datetime.now()
    return now.strftime("%Y-%m-%d_%H-%M-%S")
