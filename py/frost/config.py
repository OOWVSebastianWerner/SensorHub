# ------------------------------------------------------------------------------
# Global config vars
# ------------------------------------------------------------------------------
#@Todo: refactor to dotenv
import os
from dotenv import load_dotenv
load_dotenv('../../.env')

baseURL = os.getenv('baseURL')

endpoints = {
    'things': f'{baseURL}Things',
    'locations': f'{baseURL}Locations',
    'sensors': f'{baseURL}Sensors',
    'observedProperties': f'{baseURL}ObservedProperties'
}