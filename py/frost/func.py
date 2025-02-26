# ------------------------------------------------------------------------------
# Functions to get data from FROST-Server
# ------------------------------------------------------------------------------
#@TODO: split functions into seperate files
#%%
import json
from .config import endpoints

# ------------------------------------------------------------------------------
# List items
# ------------------------------------------------------------------------------
def list_items(session, item_type) -> json:
    """List all entries for given item_type.
    returns json
    """
    url = f'{endpoints[item_type]}'
    
    return session.get(url).json()
    
# ------------------------------------------------------------------------------
# Get items
# ------------------------------------------------------------------------------
def get_thing(session, thing_id=None) -> json:
    """ Get thing as json for given id.
    returns json
    """
    url = f'{endpoints['things']}({thing_id})'
    
    return session.get(url).json()

def get_thing_id(session, key, value) -> json:
    """ Get thing id from FROST-Server."""
    qry_thing = (
    f"{endpoints['things']}"
    f"?$filter={key} eq '{value}'&$select=@iot.selfLink")

    return session.get(qry_thing).json()
    

def get_foreign_id(session, foreign_id, id_fld):
    """Select things id by foreign id from FROST-Server.
    
    Returns None if there is no entry with given foreign id."""
    filter = f"?$filter={id_fld} eq '{foreign_id}'&$select=@iot.id"
    
    r = session.get(f'{endpoints['things']}{filter}').json()
    
    if len(r['value']) > 0:
       return r.get('value')[0].get('@iot.id')
    else:
        return None

def get_location_id(session, thing_id):

    url = f'{endpoints['things']}({thing_id})/Locations?$select=@iot.id'

    r = session.get(url).json()
    
    if len(r['value']) > 0:
        return r.get('value')[0].get('@iot.id')
    else:
        return None
    
def get_datastream_id(session, thing_id):

    url = f'{endpoints['things']}({thing_id})/Datastreams?$select=@iot.id'

    r = session.get(url).json()
    
    if len(r['value']) > 0:
        return r.get('value')[0].get('@iot.id')
    else:
        return None
    
def get_sensor(session, sensor_id=None):

    url = f'{endpoints['sensors']}({sensor_id})'

    return session.get(url).json()

def get_observedProperties(session, oP_id=None):
    
    url = f'{endpoints['observedProperties']}({oP_id})'
    
    return session.get(url).json()

# ------------------------------------------------------------------------------
# add items
# ------------------------------------------------------------------------------

def add_thing(session, json_data):
    """Add a new thing to FROST-Server."""
    
    url = f'{endpoints['things']}'
    response = session.post(url, json_data)

    return response

def add_location(session, thing_url, json_data):
    """Add a new location of a thing to FROST-Server."""
    
    url = f'{thing_url})/Locations'
    response = session.post(url, json_data)

    return response

def add_datastream(session, thing_id, json_data):
    """Add a new datastream to FROST-Server."""

    url = f'{endpoints['things']}({thing_id})/Datastreams'
    response = session.post(url, json_data)

    return response
# ------------------------------------------------------------------------------
# update items
# ------------------------------------------------------------------------------

def update_thing(session, thing_id, json_data):
    """Update thing in FROST-Server."""
        
    url = f'{endpoints['things']}({thing_id})'
    response = session.patch(url, json_data)

    return response

def update_location(session, thing_id, json_data):
    """Update location of a thing in FROST-Server.
    
    If thing has no location a location is added.
    """
    
    location_id = get_location_id(session, thing_id)

    if location_id:
        print('Updating existing location.')
        url = f'{endpoints['locations']}({location_id})'
        response = session.patch(url, json_data)

        return response
    
    else:
        print(f'Found no location for thing {thing_id}.\nAdd new location.')
        url = f'{endpoints['things']}({thing_id})/Locations'
        response = session.post(url, json_data)

        return response

def update_datastream(session, thing_id, json_data):
    """Update a datastream of a thing in FROST_Server."""
    
    datastream_id = get_datastream_id(session, thing_id)

    if datastream_id:
        print('Updating existing datastream.')
        url = f'{endpoints['things']}({thing_id})/Datastreams({datastream_id})'
        response = session.patch(url, json_data)
        
        return response
    
    else:
        print('No datastream found. Add new datastream.')
        url = f'{endpoints['things']}({thing_id})/Datastreams'
        response = session.post(url, json_data)

        return response

# ------------------------------------------------------------------------------
# helper functions
# ------------------------------------------------------------------------------
def save_response_as_file(response, filename):
    with open(filename, 'w') as output_file:
        output_file.write(json.dumps(response.json(), indent=4))
# %%
