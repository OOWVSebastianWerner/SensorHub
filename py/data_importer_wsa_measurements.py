#------------------------------------------------------------------------------
# Name:         post measurements to FROST-Server
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
from frost import frost_config
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%

basePath = Path(r'..\data')
levelsPath = Path(basePath / r'wsa\levels')

baseUrl_wsa = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/'

qry_things = (
    f"{frost_config.baseURL}{frost_config.endpoints['things']}"
    "?$select=@iot.id,properties/uuid"
    "&$expand=Datastreams($select=@iot.id,@iot.selfLink)"
)

# POST-function for parallel processing
def post_observations(session, datastream, data):
    responses = []
    for _, row in data.iterrows():
        row_dict = row.to_dict()
        response = session.post(f'{datastream}/Observations', json=row_dict)
        responses.append((response.status_code, response.text))
    return responses

#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------
#%%
with requests.Session() as session:
    # Set global header
    session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})

    # get things from FROST-Server
    things = session.get(qry_things).json()

    for thing in things['value']:
        id_ = thing['@iot.id']
        uuid = thing['properties']['uuid']
        datastream = thing['Datastreams'][0]['@iot.selfLink']

        datastream_res = session.get(f'{datastream}').json()
        # get last phenomenonTime in Datastream
        if 'phenomenonTime' in datastream_res.keys():
            # lastEntryTime = datetime.fromisoformat(
            #     datastream_res['phenomenonTime'].split('/')[1]
            # )
            lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
        else:
            lastEntryTime = None

        print(f"Processing Thing ID: {id_}, StartTime: {lastEntryTime}, Datastream: {datastream}")
        # # !!! FOR DEV ONLY !!!
        # if uuid == '47174d8f-1b8e-4599-8a59-b580dd55bc87':
            # get measurements
        response = session.get(f'{baseUrl_wsa}stations/{uuid}/W/measurements.json')
        if response.status_code == 200:
            # create DataFrame
            df = pd.DataFrame(response.json())
            df.rename(columns={'timestamp': 'phenomenonTime', 'value': 'result'}, inplace=True)
            #df['phenomenonTime'] = pd.to_datetime(df['phenomenonTime'])

            if lastEntryTime:
                df_to_post = df[df['phenomenonTime'] > lastEntryTime]
            else:
                df_to_post = df
            
            # Parallelisierung für POST-Anfragen
            with ThreadPoolExecutor() as executor:
                results = executor.submit(post_observations, session, datastream, df_to_post)
            
            # Ergebnisse prüfen
            for status_code, message in results.result():
                if status_code == 200 or status_code == 201:
                    print(f"Observation posted successfully: {message}")
                else:
                    print(f"Error posting observation: {status_code}, {message}")

# %%
