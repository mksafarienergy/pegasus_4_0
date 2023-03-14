import json
import time
import warnings
from math import floor
from dateutil import rrule
from datetime import datetime

import pandas as pd

from src.dynamics.dynamics_auth import create_session, use_dynamics_session
from utils import get_time_now

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

API_URL = 'https://safarienergy.api.crm.dynamics.com/api/data/v9.2/'

month_energy_list = ['jan_energy', 'feb_energy', 'mar_energy', 'apr_energy', 'may_energy', 'jun_energy',
                     'jul_energy', 'aug_energy', 'sep_energy', 'oct_energy', 'nov_energy', 'dec_energy']
month_insolation_list = ['jan_inso', 'feb_inso', 'mar_inso', 'apr_inso', 'may_inso', 'jun_inso',
                         'jul_inso', 'aug_inso', 'sep_inso', 'oct_inso', 'nov_inso', 'dec_inso']
month_name_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def get_assets(session) -> pd.DataFrame:
    """
    Getting data from the asset table in Dynamics. Cleaning up the data slightly to 
        remove uneccessary information and formatting it.

    :param session: session for the connection to the Dynamics API
    :return: DataFrame with asset information and another DataFrame with same data
    """
    res = use_dynamics_session(session, API_URL+'cre90_legalrequests?$select=activityid,subject,actualend,_cre90_counterparty_value,description,cre90_note,_cre90_assignedto_value,cre90_requesttype,cre90_sharepointlink\
        &$expand=cre90_assignedto/systemuserid')
        # &$expand=cre90_assignedto_systemusers/$ref($select=fullname)')
        # cre90_msdyn_purchaseorderproduct/Id
    res_json = res.content.decode('utf-8')
    asset_data = json.loads(res_json)
    print(asset_data)
    # df_assets = pd.DataFrame(asset_data['value'])
    # df_assets.to_csv('csvs/asset_data')

    return pd.DataFrame()



if __name__ == '__main__':
    session = create_session()
    proforma_df = get_assets(session)
