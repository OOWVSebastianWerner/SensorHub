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
from pathlib import Path
import frost
import frost.func
import frost.models

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%
basePath = Path(r'..\data\wsa')

stationsUrl = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json'

#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------
#%%
try:
    with requests.Session() as s:
    
        s.headers.update({'content-type': 'application/json; charset=UTF-8'})
        s.headers.update({'Accept': 'application/json'})
        
        loaded_stations = s.get(stationsUrl)

        if loaded_stations.status_code == 200:
            
            stations_json = loaded_stations.json()
            
            for i in stations_json:
                # get foreign id from loaded stations
                foreign_id = i.get('uuid')
                                
                station = frost.models.Thing(i.get('shortname'), 'water_station', foreign_id)
                
                for key in i.keys():
                    if key not in ['uuid', 'longitude', 'latitude']:
                        station.add_property([key, i.get(key)])

                if i.get('longitude') and i.get('latitude'):
                    location = frost.models.Location(i.get('shortname'), i.get('latitude'), i.get('longitude'))
                else:
                    location = None

                thing_id = frost.func.get_foreign_id(s, foreign_id, 'properties/foreign_id')

                if thing_id:
                    print(f'Update thing: {station.name} (ID: {thing_id})')
                    # Update...
                    res_thing = frost.func.update_thing(s, thing_id, station.to_json())
                    # get url of updated thing from response header
                    thing_url = res_thing.headers.get('Location')
                    print(res_thing.status_code, res_thing.text, thing_url)
                    # Update Location
                    if location:
                        location.link_thing(thing_id)
                        res_location = frost.func.update_location(s, thing_id, location.to_json())
                        print(res_location.status_code, res_location.text)
                    
                    datastream_id = frost.func.get_datastream_id(s, thing_id)
                    datastream = frost.models.Datastream(f'Water level {station.name}', thing_id, 1, 3)
                    datastream.unitOfMeasurement['name'] = 'centimeter'
                    datastream.unitOfMeasurement['symbol'] = 'cm'

                    if datastream_id:
                        res_datastream = frost.func.update_datastream(s, thing_id, datastream.to_json())
                    else:
                        res_datastream = frost.func.add_datastream(s, thing_id, datastream.to_json())

                else:
                    print(f'Add thing: {station.name}')
                    r = frost.func.add_thing(s, station.to_json())

                    if r.status_code == 201:
                        # Get thing URL from response header
                        thing_url = r.headers.get('Location')
                        # extract thing id from url
                        thing_id = thing_url.split('/')[-1].split('(')[-1][:-1]
                        
                        if location:
                            # Link location with thing
                            location.link_thing(thing_id)
                            print('Add location')
                            r_location = frost.func.add_location(s, thing_url, location.to_json())
                        
                        datastream = frost.models.Datastream(f'Water level {station.name}', thing_id, 1, 3)
                        datastream.unitOfMeasurement['name'] = 'centimeter'
                        datastream.unitOfMeasurement['symbol'] = 'cm'

                        r_datastream = frost.func.add_datastream(s, thing_id, datastream.to_json())

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')
# %%
