#------------------------------------------------------------------------------
# Name:         data importer nlwkn stations
#               Imports the nlwkn stations into the FROST-Server
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      04.02.2025
#------------------------------------------------------------------------------
#%%
import os
from dotenv import load_dotenv
import requests
import frost
import frost.func
import frost.models
from tqdm import tqdm

load_dotenv
#------------------------------------------------------------------------------
#--- global vars

# Public API Key
api_key = os.getenv('NLWKN_API_KEY')
nlwkn_stations_url = os.getenv('NLWKN_STATIONS_URL')

stationsUrl = f'{nlwkn_stations_url}?key={api_key}'
#%%
# values that will be added to properties of thing
property_keys = [
    'Betreiber',
    'MS_FOK_mNHN',
    'MS_FUK_mNHN',
    'MS_GOK_mNHN',
    'MS_MBP_mNHN',
]
# keys for latitude and longitude in incoming json
key_lat = 'WGS84Rechtswert'
key_lon = 'WGS84Hochwert'

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
            
            for i in tqdm(stations_json['getStammdatenResult'], desc='Loading nlwkn stations...', ascii=False, ncols=75):
                # Only take stations with location (coordinates)
                if i.get(key_lat) and i.get(key_lon):
                    foreign_id = i.get('STA_ID')               
                    station = frost.models.Thing(i.get('Name'), 'groundwater_station', foreign_id)
                    
                    for key in property_keys:
                        station.add_property([key, i.get(key)])

                    location = frost.models.Location(i.get('Name'), i.get(key_lat), i.get(key_lon))

                    thing_id = frost.func.get_foreign_id(s, foreign_id, 'properties/foreign_id')

                    if thing_id:
                        # print(f'Update thing: {station.name} (ID: {thing_id})')
                        # Update...
                        res_thing = frost.func.update_thing(s, thing_id, station.to_json())
                        # get url of updated thing from response header
                        thing_url = res_thing.headers.get('Location')
                        # print(res_thing.status_code, res_thing.text, thing_url)
                        # Update Location
                        location.link_thing(thing_id)
                        res_location = frost.func.update_location(s, thing_id, location.to_json())
                        # print(res_location.status_code, res_location.text)
                        # Update Datastream
                        datastream_id = frost.func.get_datastream_id(s, thing_id)
                        datastream = frost.models.Datastream(f'Groundwater level {station.name}', thing_id, 1, 3)
                        datastream.unitOfMeasurement['name'] = 'meter'
                        datastream.unitOfMeasurement['symbol'] = 'm'

                        if datastream_id:
                            res_datastream = frost.func.update_datastream(s, thing_id, datastream.to_json())
                            #print(res_datastream.status_code)

                    else:
                        # print(f'Add thing: {station.name}')
                        r = frost.func.add_thing(s, station.to_json())

                        if r.status_code == 201:
                            # Get thing URL from response header
                            thing_url = r.headers.get('Location')
                            # extract thing id from url
                            thing_id = thing_url.split('/')[-1].split('(')[-1][:-1]

                            # Link location with thing
                            location.link_thing(thing_id)

                            r_location = frost.func.add_location(s, thing_url, location.to_json())
                            
                            datastream = frost.models.Datastream(f'Groundwater level {station.name}', thing_id, 1, 3)
                            datastream.unitOfMeasurement['name'] = 'meter'
                            datastream.unitOfMeasurement['symbol'] = 'm'

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
