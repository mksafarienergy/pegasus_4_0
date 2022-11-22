from dynamics_auth import create_session, use_dynamics_session
import pandas as pd


API_URL = 'https://safarienergy.api.crm.dynamics.com/api/data/v9.2/'






# if __name__ == "__main__":
#     print("Hello World!")
#     session = create_session()
#     res = use_dynamics_session(session, API_URL+'msdyn_customerassets?$select=msdyn_name,cre90_projectnumber,cre90_monitoringplatform,cre90_monitoringplatformid,msdyn_customerassetid,cre90_availableinpegasus,cre90_opportunityaddress,cre90_timezone')
#     df = pd.read_json(res.json())
#     print(df)



"""
def get_proforma_dataframe(session, df_assets):
    #Request Asset Pro Forma Data
    pro_requests_url = API_URL+'cre90_assetproformas?$select=\
                                cre90_name,\
                                cre90_apistartdate,\
                                _cre90_asset_value,\
                                cre90_degradationprofile,\
                                cre90_receivedexception,\
                                cre90_netexception,\
                                cre90_januaryenergy,\
                                cre90_februaryenergy,\
                                cre90_marchenergy,\
                                cre90_aprilenergy,\
                                cre90_mayenergy,\
                                cre90_juneenergy,\
                                cre90_julyenergy,\
                                cre90_augustenergy,\
                                cre90_septemberenergy,\
                                cre90_octoberenergy,\
                                cre90_novemberenergy,\
                                cre90_decemberenergy,\
                                cre90_januaryinsolation,\
                                cre90_februaryinsolation,\
                                cre90_marchinsolation,\
                                cre90_aprilinsolation,\
                                cre90_mayinsolation,\
                                cre90_juneinsolation,\
                                cre90_julyinsolation,\
                                cre90_augustinsolation,\
                                cre90_septemberinsolation,\
                                cre90_octoberinsolation,\
                                cre90_novemberinsolation,\
                                cre90_decemberinsolation,\
                                _cre90_backupasset_value'
    r_pro = session.get(pro_requests_url)
    pro_json = r_pro.content.decode('utf-8')
    pro_data = json.loads(pro_json)
    df_pro = pd.DataFrame(pro_data['value']).drop(['@odata.etag'],axis=1)
    df_pro = df_pro.rename(columns={'_cre90_asset_value': 'asset_id',
                                    'cre90_name': 'asset_name',
                                    'cre90_degradationprofile': 'degradation_profile',
                                    'cre90_apistartdate': 'api_start_date',
                                    'cre90_receivedexception': 'received_exception',
                                    'cre90_netexception': 'net_exception',
                                    'cre90_januaryenergy': 'jan_energy',
                                    'cre90_februaryenergy': 'feb_energy',
                                    'cre90_marchenergy': 'mar_energy',
                                    'cre90_aprilenergy': 'apr_energy',
                                    'cre90_mayenergy': 'may_energy',
                                    'cre90_juneenergy': 'jun_energy',
                                    'cre90_julyenergy': 'jul_energy',
                                    'cre90_augustenergy': 'aug_energy',
                                    'cre90_septemberenergy': 'sep_energy',
                                    'cre90_octoberenergy': 'oct_energy',
                                    'cre90_novemberenergy': 'nov_energy',
                                    'cre90_decemberenergy': 'dec_energy',
                                    'cre90_januaryinsolation': 'jan_inso',
                                    'cre90_februaryinsolation': 'feb_inso',
                                    'cre90_marchinsolation': 'mar_inso',
                                    'cre90_aprilinsolation': 'apr_inso',
                                    'cre90_mayinsolation': 'may_inso',
                                    'cre90_juneinsolation': 'jun_inso',
                                    'cre90_julyinsolation': 'jul_inso',
                                    'cre90_augustinsolation': 'aug_inso',
                                    'cre90_septemberinsolation': 'sep_inso',
                                    'cre90_octoberinsolation': 'oct_inso',
                                    'cre90_novemberinsolation': 'nov_inso',
                                    'cre90_decemberinsolation': 'dec_inso',
                                    '_cre90_backupasset_value': 'backup_asset_id'})
"""