
#%%
import requests
from pathlib import Path
import pandas as pd
from datetime import datetime
from frost import config
import zipfile
import io
import os
import time


# Alle Stations ID's abrufen vom Frostserver
#%%

base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/"
output_dir = r'C:\Users\User\Desktop\stundendaten'

stations = [
    '00078',  
    '00053',    
]

qry_ids = (
    f"{config.endpoints['things']}"
    "?$filter=properties/station_type eq 'raing gauge'"
    "&$select=@iot.id,properties/foreign_id"
)


#%%

with requests.Session() as session:
    # Set global header
    session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})

    # get things from FROST-Server
    things = session.get(qry_ids).json()
    
    # FROST-Server limits requests to 100 entries by default
    # as long as there is '@iot.nextLink' present in things, do another request 
    # and combine it with things
    while '@iot.nextLink' in things.keys():
        things = things | session.get(things['@iot.nextLink']).json()
        things.pop('@iot.nextLink')

    for thing in things['value']:
        id_ = thing['@iot.id']
        uuid = thing['properties']['foreign_id']

#%%
print(things)

#%%
def download_and_unzip_file(station_id):
    file_name = f"stundenwerte_RR_{station_id}_akt.zip"
    url = base_url + file_name

    try:
        print(f"Downloading {file_name} from {url}")  #  Print der Anfrage
        response = requests.get(url)  # Anfrage an den Zielserver
        # Jede Antwort enthält einen Status-Code vom Server (z.B. 404)
        # die Funtkion wirft einen Fehler, sobald eine Antwort nicht den Code 200 (für alles ok) hat.
        response.raise_for_status()  # Überprüfe, ob der Download erfolgreich war

        # Zip-Datei in den Arbeitsspeicher laden
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(os.path.join(output_dir, station_id))
            print(f"Extracted {file_name} to {output_dir}/{station_id}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP Fehler anzeigen
    except Exception as err:
        print(f"Other error occurred: {err}")  # Andere Fehler anzeigen

def main():
    while True:
        for station in stations:
            download_and_unzip_file(station)
        
        # Warte eine Minute
        print("Waiting for the next cycle...")
        time.sleep(60)

if __name__ == "__main__":
    main()

#%%

# with requests.Session() as session:
#     # Set global header
#     session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})

#     # get things from FROST-Server
#     things = session.get(qry_ids).json()
    
#     # FROST-Server limits requests to 100 entries by default
#     # as long as there is '@iot.nextLink' present in things, do another request 
#     # and combine it with things
#     while '@iot.nextLink' in things.keys():
#         things = things | session.get(things['@iot.nextLink']).json()
#         things.pop('@iot.nextLink')

#     for thing in things['value']:
#         id_ = thing['@iot.id']
#         uuid = thing['properties']['foreign_id']

























# qry_things = (
#     f"{config.endpoints['things']}"
#     "?$filter=properties/station_type eq 'raingauge_station'"
#     "&$select=@iot.id,properties/foreign_id"
#     "&$expand=Datastreams($select=@iot.id,@iot.selfLink)"
# )

# # POST-function
# def post_observations(session, datastream, data):
#     responses = []
#     for _, row in data.iterrows():
#         row_dict = row.to_dict()
#         response = session.post(f'{datastream}/Observations', json=row_dict)
#     responses.append((response.status_code, response.text))
#     return responses






# with requests.Session() as session:
#     # Set global header
#     session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})

#     # get things from FROST-Server
#     things = session.get(qry_things).json()
    
#     # FROST-Server limits requests to 100 entries by default
#     # as long as there is '@iot.nextLink' present in things, do another request 
#     # and combine it with things
#     while '@iot.nextLink' in things.keys():
        
#         nextLink = things['@iot.nextLink']
#         next_things = session.get(nextLink).json()

#         things['value'] += next_things['value']

#         if '@iot.nextLink' in next_things.keys():
#             things['@iot.nextLink'] = next_things['@iot.nextLink']
#         else:
#             things.pop('@iot.nextLink')


#     for thing in things['value']:
#         id_ = thing['@iot.id']
#         uuid = thing['properties']['foreign_id']
#         # IMPORTANT: only works when there is only one datastream per thing
#         # @TODO: add filter/check to get the correct datastream if there is more than one.
#         datastream = thing['Datastreams'][0]['@iot.selfLink']

#         datastream_res = session.get(f'{datastream}').json()
#         # get last phenomenonTime in Datastream
#         if 'phenomenonTime' in datastream_res.keys():
#             lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
#         else:
#             lastEntryTime = None

#         print(f"Processing Thing ID: {id_}, Start: {datetime.now()}, Import: {lastEntryTime}, Datastream: {datastream}")
        
#         response = session.get(f'{baseUrl_wsa}stations/{uuid}/W/measurements.json')
        
#         if response.status_code == 200 and response.json():
#             # create DataFrame
#             df = pd.DataFrame(response.json())
#             df.rename(columns={'timestamp': 'phenomenonTime', 'value': 'result'}, inplace=True)

#             if lastEntryTime:
#                 df_to_post = df[df['phenomenonTime'] > lastEntryTime][-5:]
#             else:
#                 df_to_post = df[-5:]
            
#             r = post_observations(session, datastream, df_to_post)
#             print(r)
# # %%

# %%
