# ------------------------------------------------------------------------------
# Functions to get data from FROST-Server
# ------------------------------------------------------------------------------
import requests
import json
from frost_config import baseURL, endpoints

# ------------------------------------------------------------------------------
# List items
# ------------------------------------------------------------------------------
def list_items(session, item_type) -> json:
    """List all entries for given item_type.
    returns json
    """
    url = f'{baseURL}{endpoints[item_type]}'
    
    return session.get(url).json()
    
# ------------------------------------------------------------------------------
# Get items
# ------------------------------------------------------------------------------
def get_thing(session, thing_id=None) -> json:
    """ Get thing as json for given id.
    returns json
    """
    url = f'{baseURL}{endpoints['things']}({thing_id})'
    
    return session.get(url).json()

def get_thing_id(session, key, value) -> json:

    qry_thing = (
    f"{baseURL}{endpoints['things']}"
    f"?$filter={key} eq '{value}'&$select=@iot.selfLink")

    thing_id = session.get(qry_thing).json()
    
    return thing_id

def get_sensor(session, sensor_id=None):

    url = f'{baseURL}{endpoints['sensors']}({sensor_id})'

    return session.get(url).json()

def get_observedProperties(session, oP_id=None):
    
    url = f'{baseURL}{endpoints['observedProperties']}({oP_id})'
    
    return session.get(url).json()

# ------------------------------------------------------------------------------
# update items
# ------------------------------------------------------------------------------

def update_thing(session, thing_id, update):

    url = f'{baseURL}{endpoints['things']}({thing_id})'

    session.patch(url, update)