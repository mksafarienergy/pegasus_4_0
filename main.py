import pandas as pd

from proforma import proforma_monthly_dataframe
from utils import get_time_now

### Dont know if i want to keep this
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)



if __name__ == '__main__':
    proforma_df = proforma_monthly_dataframe()
    df = proforma_df.head()
