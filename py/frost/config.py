# ------------------------------------------------------------------------------
# Global config vars
# ------------------------------------------------------------------------------
#@Todo: refactor to dotenv
#%%
import os
from dotenv import load_dotenv
load_dotenv('../../.env', override=True)

baseURL = os.getenv('FROST_URL_LOCAL')

endpoints = {
    'things': f'{baseURL}Things',
    'locations': f'{baseURL}Locations',
    'sensors': f'{baseURL}Sensors',
    'observedProperties': f'{baseURL}ObservedProperties',
    'datastreams': f'{baseURL}Datastreams'
}
# %%
