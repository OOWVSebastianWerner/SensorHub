# ------------------------------------------------------------------------------
# Global config vars
# ------------------------------------------------------------------------------
#@Todo: refactor to dotenv
#%%
import os

# baseURL = 'http://frost-server:8080/FROST-Server/v1.1/'
baseURL = os.getenv('FROST_SERVER_URL')


endpoints = {
    'things': f'{baseURL}Things',
    'locations': f'{baseURL}Locations',
    'sensors': f'{baseURL}Sensors',
    'observedProperties': f'{baseURL}ObservedProperties'
}
# %%
