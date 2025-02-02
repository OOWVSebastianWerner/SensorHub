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
import os
import pandas as pd
from pathlib import Path
import json
# from frost import config
import frost
# from frost import config, func
#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%
basePath = Path(r'..\data\wsa')

stationsUrl = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json'

#------------------------------------------------------------------------------
#--- functions
#------------------------------------------------------------------------------

def update_location(session, thing_id, json_data):
    """Update thing"""
    
    url = rf'{frost.config.endpoints['things']}({thing_id})\Locations'
    
    r = session.patch(url, json_data)

    return (r.status_code, r.text)
#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------
#%%
try:
    with requests.Session() as s:
    
        s.headers.update({'content-type': 'application/json; charset=UTF-8'})
        
        loaded_stations = s.get(stationsUrl)

        if loaded_stations.status_code == 200:
            
            stations_json = loaded_stations.json()
            
            for i in stations_json:
                foreign_id = i.get('uuid')
                                
                station = frost.Thing(i.get('shortname'), 'water_station', foreign_id)

                location = frost.Location(i.get('shortname'), i.get('latitude'), i.get('longitude'))
               
                for key in i.keys():
                    if key not in ['uuid', 'longitude', 'latitude']:
                        station.add_property([key, i.get(key)])

                # location_dict = {
                #     'name': i.get('longname'), 
                #     'description': f"{i.get('water').get('longname')} at km {i.get('km')}",
                #     'encodingType': "application/geo+json", 
                #     'location': { 
                #         'type': 'Point', 
                #         'coordinates': [i.get('longitude'), i.get('latitude')]
                #         },
                # }
                # datastream_dict = {
                #     'name': f"water levels station {i.get('shortname')}",
                #     'description': "",
                #     'observationType': "",
                #     "unitOfMeasurement": {
                #         'name': 'centimeter',
                #         'symbol': 'cm',
                #         'definition': 'na'
                #     },
                #     'Sensor': {"@iot.id": 1},
                #     'ObservedProperty': {"@iot.id": 3}
                # }

                thing_id = frost.func.get_foreign_id(s, foreign_id, 'properties/foreign_id')

                # if thing_id:
                #     print(f'Update thing: {station.name}')
                #     # Update...
                #     print(frost.func.update_thing(s, thing_id, station.to_json()))
                # else:
                #     print(f'Add thing: {station.name}')
                #     print(frost.func.add_thing(s, station.to_json()))

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')
# %%
