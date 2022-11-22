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
    res = locus_api.use_locus_session('https://api.locusenergy.com/v3/sites/3888881/data?fields=Wh_sum&start=2022-3-7T00:00:00&end=2022-3-29T00:00:00&tz=UTC&gran=daily')
    print(res)
    # proforma_df = proforma_monthly_dataframe()
    
    # now = get_time_now()
    # proforma_df.to_csv(f'csvs/proforma_df_{now}.csv')
