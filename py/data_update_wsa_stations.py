#------------------------------------------------------------------------------
# Name:         data updater wsa stations
#               Updates stations in FROST-Server
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      12.12.2024
#------------------------------------------------------------------------------
#%%
import requests
import pandas as pd
from pathlib import Path
import json
from frost import frost_config
#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%
#server = r'http://localhost:8080/FROST-Server/v1.1/'

#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------
def find_thing_id(session, key, value) -> json:
    qry_thing = (
    f"{frost_config.baseURL}{frost_config.endpoints['things']}"
    f"?$filter={key} eq '{value}'&$select=@iot.selfLink")

    thing_id = session.get(qry_thing).json()
    
    return thing_id

def update_thing(thing_id, entry):
    update_qry = ""
    return None

def update_location():
    return None

def update_datastream():
    return None

with requests.session() as session:
    id = find_thing_id(session,'name', 'EITZE')
    print(id)
    id_by_uuid = find_thing_id(session, 'properties/uuid', '5aaed954-de4e-4528-8f65-f3f530bc8325')
    print(id_by_uuid)
# %%
