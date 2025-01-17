#------------------------------------------------------------------------------
# Name:         update (nlwkn) stations
#               Update existing stations (things, location, datastream, etc.)
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      16.01.2025
#------------------------------------------------------------------------------
#%%
import requests
import json
from pathlib import Path
from frost import frost_config

#%%
def get_thing_id(session, foreign_id, id_fld):
    
    filter = f'?$filter={id_fld} eq {foreign_id}&$select=@iot.id'
    
    r = session.get(f'{frost_config.baseURL}{frost_config.endpoints['things']}{filter}').json()
    if len(r['value']) > 0:
        thing_id = r.get('value')[0].get('@iot.id')
    else:
        thing_id = None
        
    return thing_id

def update_thing(session, thing_id, update_json):
    """Update thing"""
    
    url = f'{frost_config.baseURL}{frost_config.endpoints['things']}({thing_id})'
    
    r = session.patch(url, update_json)

    return r.status_code

#%%
basePath = Path(r'..\data\nlwkn')

input_File = Path(basePath / 'nlwkn_data.json')

with open(input_File, 'r') as f:
    data = json.load(f)

with requests.session() as s:
    
    for station in data['getStammdatenResult']:
        foreign_id = station.get('STA_ID')
        id = get_thing_id(s, foreign_id, 'properties/id')

        station_dict = {
            'name': station.get('Name'),
            'description': "",
            'properties': {
                'station_type': "groundwater_station",
                'foreign_id': station.get('STA_ID'),
                'betreiber': station.get('Betreiber'),
                'FOK_mNHN': station.get('MS_FOK_mNHN'),
                'FUK_mNHN': station.get('MS_FUK_mNHN'),
                'GOK_mNHN': station.get('MS_GOK_mNHN'),
                'MBP_mNHN': station.get('MS_MBP_mNHN')
            },
        }
        print(update_thing(s, id, json.dumps(station_dict, indent=4, default=str)))
    
# %%
        # 'Locations': [
        #     {
        #     'name': station.get('Name'), 
        #     'description': "",
        #     'encodingType': "application/geo+json", 
        #     'location': { 
        #         'type': 'Point', 
        #         'coordinates': [station.get('WGS84Hochwert'), station.get('WGS84Rechtswert')]
        #         },
        #     }
        # ],
        # 'Datastreams': [
        #     {
        #         'name': f"groundwater levels station {station.get('Name')}",
        #         'description': "",
        #         'observationType': "",
        #         "unitOfMeasurement": {
        #             'name': 'meter NHN',
        #             'symbol': 'm NHN',
        #             'definition': 'na'
        #         },
        #         'Sensor': {"@iot.id": 1},
        #         'ObservedProperty': {"@iot.id": 3}
        #     }
        # ]
