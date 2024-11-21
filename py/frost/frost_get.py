# ------------------------------------------------------------------------------
# Functions to get data from FROST-Server
# ------------------------------------------------------------------------------
import requests
from .frost_config import baseURL, endpoints

def thing(thing_id=None):
    if thing_id:
        url = f'{baseURL}{endpoints['things']}({thing_id})'
    else:
        url = f'{baseURL}{endpoints['things']}'
    
    return requests.get(url)

def sensor(sensor_id=None):
    if sensor_id:
        url = f'{baseURL}{endpoints['sensors']}({sensor_id})'
    else:
        url = f'{baseURL}{endpoints['sensors']}'

    return requests.get(url)

def observedProperty(oP_id=None):
    if oP_id:
        url = f'{baseURL}{endpoints['observedProperty']}({oP_id})'
    else:
        url = f'{baseURL}{endpoints['observedProperty']}'
    
    return requests.get(url)
