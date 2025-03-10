#------------------------------------------------------------------------------
# Name:         data importer dwd stations
#               Imports the dwd stations into the FROST-Server
#
# author:       Ivo Sieghold
# email:        i.sieghold@oowv.de
# created:      20.11.2024
#------------------------------------------------------------------------------
#%%
import os
from dotenv import load_dotenv
import requests
import frost
import frost.func
import frost.models
import pandas as pd

load_dotenv

dwd_stations_url = os.getenv('DWD_STATIONS_URL')
# %%

try:
    with requests.Session() as s:
        s.headers.update({'content-type': 'application/json; charset=UTF-8'})
        s.headers.update({'Accept': 'application/json'})
        
        # get stations txt-file
        dwd_stations = s.get(dwd_stations_url)

        # lines from text-file into list
        lines = dwd_stations.content.decode('cp1252').split('\r\n')
        
        station_rows = []
        
        for line in lines:

            parts = line.split()

            if parts:
                # extract fields
                stations_id = parts[0]
                von_datum = parts[1]
                bis_datum = parts[2]
                stationshoehe = parts[3]
                geobreite = parts[4]
                geolaenge = parts[5]
                bundesland = parts[-2]
                abgabe = parts[-1]
                stationsname = " ".join(parts[6:-2])
                
                station_rows.append([
                    stations_id, von_datum, bis_datum, stationshoehe, geobreite, 
                    geolaenge, stationsname, bundesland, abgabe
                ])

        spaltennamen = [
            "Stations_id", "von_datum", "bis_datum", "Stationshoehe", "geoBreite", 
            "geoLaenge", "Stationsname", "Bundesland", "Abgabe"
        ]

        df = pd.DataFrame(station_rows[2:], columns=spaltennamen)

        df['Stations_id'] = df['Stations_id'].astype(int)
        df['von_datum'] = df['von_datum'].astype(int)
        df['bis_datum'] = df['bis_datum'].astype(int)
        df['Stationshoehe'] = df['Stationshoehe'].astype(float)
        df['geoBreite'] = df['geoBreite'].astype(float)
        df['geoLaenge'] = df['geoLaenge'].astype(float)

        df = df[df['Abgabe']== 'Frei']
        # Limit to stations that have data in 2025
        df = df[df['bis_datum'] > 20250000]

        dict_data = df.to_dict(orient='records')

        for info in dict_data: # tqdm(dict_data, desc='Loading dwd stations...', ascii=False, ncols=75):
            if info.get('geoBreite') and info.get('geoLaenge'):
                foreign_id = info.get('Stations_id')
                station = frost.models.Thing(info.get('Stationsname'), 'raingauge_station', foreign_id)

                for key in info.keys():
                    if key not in ['Stations_id','geoBreite','geoLaenge']:
                        station.add_property([key,info.get(key)])
            
                location = frost.models.Location(info.get('Stationsname'), info.get('geoBreite'), info.get('geoLaenge'))

                thing_id = frost.func.get_foreign_id(s, foreign_id, 'properties/foreign_id')
                # if thing already exist in FROST-Server update thing...
                if thing_id:
                    print(f'Update thing: {station.name} (ID: {thing_id})')
                    # Update...
                    res_thing = frost.func.update_thing(s, thing_id, station.to_json())
                    # get url of updated thing from response header
                    thing_url = res_thing.headers.get('Location')
                    print('Update thing: ', res_thing.status_code, res_thing.text, thing_url)
                    # Update Location
                    if location:
                        location.link_thing(thing_id)
                        res_location = frost.func.update_location(s, thing_id, location.to_json())
                        print('Update location: ', res_location.status_code, res_location.text)
                    
                    datastream_id = frost.func.get_datastream_id(s, thing_id)
                    datastream = frost.models.Datastream(f'Rain high {station.name}', thing_id, 1, 3)
                    datastream.unitOfMeasurement['name'] = 'millimeter'
                    datastream.unitOfMeasurement['symbol'] = 'mm'

                    if datastream_id:
                        res_datastream = frost.func.update_datastream(s, thing_id, datastream.to_json())
                    else:
                        res_datastream = frost.func.add_datastream(s, thing_id, datastream.to_json())
                
                # if thing doesn't exist in FROST-Server add thing
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
                        
                        datastream = frost.models.Datastream(f'Rain high {station.name}', thing_id, 1, 3)
                        datastream.unitOfMeasurement['name'] = 'millimeter'
                        datastream.unitOfMeasurement['symbol'] = 'mm'

                        r_datastream = frost.func.add_datastream(s, thing_id, datastream.to_json())
                        print(r_datastream)

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')
# %%
