#------------------------------------------------------------------------------
# Name:         data importer wsa stations
#               Imports the wsa stations into the FROST-Server
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      07.11.2024
#------------------------------------------------------------------------------
#%%
import requests
import pandas as pd
from pathlib import Path
import json

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%
server = r'http://localhost:8080/FROST-Server/v1.1/'
basePath = Path(r'..\data\wsa')

input_File = Path(basePath / 'pegel_stations.json')

#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------

df = pd.read_json(input_File)
# %%

for i in df.index:
    station_dict = {
        'name': df.loc[i].shortname,
        'description': "",
        'properties': {
            'station type': "water station",
            'uuid': df.loc[i].uuid,
            'longname': df.loc[i].longname,
            'number': df.loc[i].number,
            'km': df.loc[i].km,
            'water_shortname': df.loc[i].water_shortname,
            'water_longname': df.loc[i].water_longname,
            'agency': df.loc[i].agency
        },
        'Locations': [
            {
            'name': df.loc[i].shortname, 
            'description': "",
            'encodingType': "application/geo+json", 
            'location': { 
                'type': 'Point', 
                'coordinates': [df.loc[i].longitude, df.loc[i].latitude]
                },
            }
        ],
        'Datastreams': [
            {
                'name': f"water levels station {df.loc[i].shortname}",
                'description': "",
                'observationType': "",
                "unitOfMeasurement": {
                    'name': 'centimeter NHN',
                    'symbol': 'cm NHN',
                    'definition': 'na'
                },
                'Sensor': {"@iot.id": 1},
                'ObservedProperty': {
                    'name': 'water level',
                    'description': 'water level',
                    'definition': ''
                }
            }
        ]
    }
    json_obj = json.dumps(station_dict, indent=4, default=str)
    
    r = requests.post(server + 'Things', json_obj)

    print(r.text)
# %%
