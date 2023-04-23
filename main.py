import time

import pandas as pd

from proforma import proforma_monthly_dataframe
from utils import get_time_now
from src.locus.locus import Locus
from src.powertrack.powertrack import Powertrack
from src.logger.logger import *

### Dont know if i want to keep this
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


if __name__ == '__main__':
    start = time.time()
    set_up_logger()

    # platform = 'locus'
    platform = 'powertrack' 

    run_time = time.time() - start
    proforma_df = pd.read_csv('proforma_df.csv')
    
    if platform == 'powertrack':
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
        # start = time.time()
        # locus.testme()
        # run_time = time.time() - start
        # log(f'Ran test: {run_time}')
        # res = locus_api.get_data_for_site('3822685', [['2022-11-10T00:00:00', '2022-11-22T00:00:00']], 'Wh_sum')
        # proforma_df = proforma_monthly_dataframe()
        
        # now = get_time_now()
        # proforma_df.to_csv(f'csvs/proforma_df_{now}.csv')

        #powertrack.mock_main(proforma_df)
        
    elif platform == 'locus':
        locus = Locus() # LocusApi()
        run_time = time.time() - start
        log(f'Initialized class: {run_time}')
        
        # start = time.time()
        # locus.testme()
        # run_time = time.time() - start
        # log(f'Ran test: {run_time}')
        # res = locus_api.get_data_for_site('3822685', [['2022-11-10T00:00:00', '2022-11-22T00:00:00']], 'Wh_sum')
        # proforma_df = proforma_monthly_dataframe()
        
        # now = get_time_now()
        # proforma_df.to_csv(f'csvs/proforma_df_{now}.csv')
        locus.mock_main(proforma_df)

    
    run_time = time.time() - start
    log(f'Finished entire script in : {run_time} seconds')

# def main():
#     # set_up_logger()
#     locus = Locus() # LocusApi()
    
#     # start = time.time()
#     # locus.testme()
#     # run_time = time.time() - start
#     # log(f'Ran test: {run_time}')
#     # res = locus_api.get_data_for_site('3822685', [['2022-11-10T00:00:00', '2022-11-22T00:00:00']], 'Wh_sum')
#     # proforma_df = proforma_monthly_dataframe()
    
#     # now = get_time_now()
#     # proforma_df.to_csv(f'csvs/proforma_df_{now}.csv')

#     proforma_df = pd.read_csv('proforma_df.csv')
#     locus.mock_main(proforma_df)
    