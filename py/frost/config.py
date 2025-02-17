# ------------------------------------------------------------------------------
# Global config vars
# ------------------------------------------------------------------------------
#@Todo: refactor to dotenv
#%%
import os
from dotenv import load_dotenv
load_dotenv('../../.env', override=True)

baseURL = os.getenv('baseURL')

baseURL = os.getenv('baseURL', 'http://localhost:8080/FROST-Server/v1.1/')

endpoints = {
    'things': f'{baseURL}Things',
    'locations': f'{baseURL}Locations',
    'sensors': f'{baseURL}Sensors',
    'observedProperties': f'{baseURL}ObservedProperties'
}
# %%
