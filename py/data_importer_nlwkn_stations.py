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
from frost import frost_config
#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%
#server = r'http://localhost:8080/FROST-Server/v1.1/'
basePath = Path(r'..\data\nlwkn')

input_File = Path(basePath / 'nlwkn_data.json')

#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------

df = pd.read_json(input_File)
with open(input_File, 'r') as f:
    data = json.load(f)

# %%

for station in data['getStammdatenResult']: #for i in df.index:
    station_dict = {
        'name': station.get('Name'), #df.loc[i].shortname,
        'description': "",
        'properties': {
            'station type': "groundwater_station",
            'id': station.get('STA_ID'),
            'betreiber': station.get('Betreiber')
        },
        'Locations': [
            {
            'name': station.get('Name'), 
            'description': "",
            'encodingType': "application/geo+json", 
            'location': { 
                'type': 'Point', 
                'coordinates': [station.get('WGS84Hochwert'), station.get('WGS84Rechtswert')]
                },
            }
        ],
        'Datastreams': [
            {
                'name': f"groundwater levels station {station.get('Name')}",
                'description': "",
                'observationType': "",
                "unitOfMeasurement": {
                    'name': 'meter NHN',
                    'symbol': 'm NHN',
                    'definition': 'na'
                },
                'Sensor': {"@iot.id": 1},
                'ObservedProperty': {"@iot.id": 3}
            }
        ]
    }
    json_obj = json.dumps(station_dict, indent=4, default=str)
    
    r = requests.post(f'{frost_config.baseURL}{frost_config.endpoints['things']}', json_obj)

    print(r.text)
# %%
