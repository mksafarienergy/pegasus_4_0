import time

import pandas as pd

from proforma import proforma_monthly_dataframe
from utils import get_time_now
from src.locus.locus import Locus
from src.powertrack.powertrack import Powertrack
from src.logger.logger import *
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    start = time.time()
    set_up_logger()

    # platform = 'locus'
    platform = 'powertrack' 

    run_time = time.time() - start
    proforma_df = pd.read_csv('proforma_df.csv')
    
    powertrack = Powertrack()
    log(f'Initialized class: {run_time}')
    site_csv = pd.read_csv('site.csv')
    site_csv = site_csv.reset_index()

    # Iterate through the rows of the DataFrame and print each column's value
    for index, row_data in site_csv.iterrows():
        print(f'row is {row_data}')
        site = row_data['site']
        start_timestamp = row_data['start_timestamp']
        end_timestamp = row_data['end_timestamp']
        binsize = row_data['timeframe']
        site_data_df = powertrack.main(site,start_timestamp,end_timestamp,binsize)
        print(f'inside main, site_data_df is {site_data_df}')

    run_time = time.time() - start
    log(f'Finished entire script in : {run_time} seconds')
