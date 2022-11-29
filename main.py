import time

import pandas as pd

from proforma import proforma_monthly_dataframe
from utils import get_time_now
from src.locus.locus import Locus

### Dont know if i want to keep this
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)



if __name__ == '__main__':
    start = time.time()
    locus = Locus() # LocusApi()
    run_time = time.time() - start
    print(f'Initialized class: {run_time}')
    
    start = time.time()
    locus.testme()
    run_time = time.time() - start
    print(f'Ran test: {run_time}')
    # res = locus_api.get_data_for_site('3822685', [['2022-11-10T00:00:00', '2022-11-22T00:00:00']], 'Wh_sum')
    # proforma_df = proforma_monthly_dataframe()
    
    # now = get_time_now()
    # proforma_df.to_csv(f'csvs/proforma_df_{now}.csv')

    # proforma_df = pd.read_csv('csvs/proforma_df_2022-11-29_15-59-47.csv')
    # locus.mock_main(proforma_df)