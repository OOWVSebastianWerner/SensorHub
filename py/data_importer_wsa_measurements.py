#------------------------------------------------------------------------------
# Name:         data get things
#               Get all things from FROST-Server and save as json
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      07.11.2024
#------------------------------------------------------------------------------
#%%
import requests
from pathlib import Path
import json
import pandas as pd

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%
basePath = Path(r'..\data')
levelsPath = Path(basePath / r'wsa\levels')

baseUrl = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/'

with open(basePath / 'curr_things.json') as things_file:
    things = json.load(things_file)

# %%
with requests.Session() as s:
    
    s.headers.update({'content-type': 'application/json; charset=UTF-8'})

    for i in things['value']:
        id = i['@iot.id']
        uuid = i['properties']['uuid']
        datastream = i['Datastreams@iot.navigationLink']

        print(id, uuid, datastream)

        r = s.get(f'{baseUrl}stations/{uuid}/W/measurements.json')

        if r.status_code == 200:
            data = r.json()

            with open(levelsPath / f'{uuid}.json', 'w') as levels_json:
                levels_json.write(json.dumps(data))
# %%
