# ------------------------------------------------------------------------------
# Functions to get data from FROST-Server
# ------------------------------------------------------------------------------
import requests
import json
from config import baseURL, endpoints

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
    """ Get thing id from FROST-Server."""
    qry_thing = (
    f"{baseURL}{endpoints['things']}"
    f"?$filter={key} eq '{value}'&$select=@iot.selfLink")

    return session.get(qry_thing).json()
    

def get_foreign_id(session, foreign_id, id_fld):
    """Get the things foreign id from FROST-Server.
    
    Returns None if there is any."""
    filter = f"?$filter={id_fld} eq '{foreign_id}'&$select=@iot.id"
    
    r = session.get(f'{endpoints['things']}{filter}').json()
    
    if len(r['value']) > 0:
       return r.get('value')[0].get('@iot.id')
    else:
        return None

def get_sensor(session, sensor_id=None):

    url = f'{baseURL}{endpoints['sensors']}({sensor_id})'

    return session.get(url).json()

def get_observedProperties(session, oP_id=None):
    
    url = f'{baseURL}{endpoints['observedProperties']}({oP_id})'
    
    return session.get(url).json()

# ------------------------------------------------------------------------------
# add items
# ------------------------------------------------------------------------------

def add_thing(session, json_data):
    """Add a new thing to FROST-Server."""
    
    url = f'{endpoints['things']}'
    
    r = session.post(url, json_data)

    return (r.status_code, r.text)
# ------------------------------------------------------------------------------
# update items
# ------------------------------------------------------------------------------

def update_thing(session, thing_id, json_data):
    """Update thing in FROST-Server."""
    
    url = f'{endpoints['things']}({thing_id})'
    
    r = session.patch(url, json_data)

    return (r.status_code, r.text)

# ------------------------------------------------------------------------------
# helper functions
# ------------------------------------------------------------------------------
def save_response_as_file(response, filename):
    with open(filename, 'w') as output_file:
        output_file.write(json.dumps(response.json(), indent=4))