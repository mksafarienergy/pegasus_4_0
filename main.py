import pandas as pd

from proforma import proforma_monthly_dataframe
from utils import get_time_now
from src.locus.locus_api import LocusApi

### Dont know if i want to keep this
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)



if __name__ == '__main__':
    locus_api = LocusApi()
    res = locus_api.get_data_for_site('3822685', ['2022-11-10T00:00:00', '2022-11-22T00:00:00'], 'Wh_sum')
    print(res)
    # proforma_df = proforma_monthly_dataframe()
    
    # now = get_time_now()
    # proforma_df.to_csv(f'csvs/proforma_df_{now}.csv')
