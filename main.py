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
        
        # start = time.time()
        # locus.testme()
        # run_time = time.time() - start
        # log(f'Ran test: {run_time}')
        # res = locus_api.get_data_for_site('3822685', [['2022-11-10T00:00:00', '2022-11-22T00:00:00']], 'Wh_sum')
        # proforma_df = proforma_monthly_dataframe()
        
        # now = get_time_now()
        # proforma_df.to_csv(f'csvs/proforma_df_{now}.csv')

        powertrack.mock_main(proforma_df)
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
    