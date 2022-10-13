import requests
import msal



# Dynamics 365 Authentication Parameters
AUTHORITY = 'https://login.microsoftonline.com/a7570dc2-3482-4ffe-9669-884a33beaa2e'
CLIENT_ID = '5db54658-c1bf-41a3-a295-37bb26bb22e3'
CLIENT_SECRET = '.E1.~~t4Q8GjVhSQCDE7r1l6JQyD6du7w.'
RESOURCE_URL = 'https://safarienergy.crm.dynamics.com/'



def _build_msal_app():
    return msal.ConfidentialClientApplication(
        client_id=CLIENT_ID, 
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET)

def _get_access_token():
    scopes = ['https://safarienergy.crm.dynamics.com/.default']
    token = _build_msal_app().acquire_token_for_client(scopes)
    return token['access_token']

def create_session():
    access_token = _get_access_token()
    session = requests.Session()
    session.headers.update(dict(Authorization=f'Bearer {access_token}'))
    return session

def use_session(session, url):
    """We should only ever GET from dynamics"""

    return session.get(url)
