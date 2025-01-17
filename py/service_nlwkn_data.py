#------------------------------------------------------------------------------
# Name:         service post nlwkn measurements to FROST-Server
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      16.01.2025
#------------------------------------------------------------------------------
#%%
import requests
from pathlib import Path
import pandas as pd
from datetime import datetime
from frost import frost_config

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%

basePath = Path(r'..\data')
api_key = '9dc05f4e3b4a43a9988d747825b39f43'

baseUrl_nlwkn = f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/stammdaten/stationen/allegrundwasserstationen?key={api_key}'

PAT_ID = 536
tage = -7

qry_things = (
    f"{frost_config.baseURL}{frost_config.endpoints['things']}"
    "?$filter=properties/station_type eq 'groundwater_station'"
    "&$select=@iot.id,properties/foreign_id"
    "&$expand=Datastreams($select=@iot.id,@iot.selfLink)"
)

# POST-function
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
    
    # FROST-Server limits requests to 100 entries by default
    # as long as there is '@iot.nextLink' present in things, do another request 
    # and combine it with things
    while '@iot.nextLink' in things.keys():
        things = things | session.get(things['@iot.nextLink']).json()
        things.pop('@iot.nextLink')

    for thing in things['value']:
        id_ = thing['@iot.id']
        foreign_id = thing['properties']['foreign_id']
        datastream = thing['Datastreams'][0]['@iot.selfLink']

        # get last phenomenonTime in Datastream
        datastream_res = session.get(f'{datastream}').json()
        if 'phenomenonTime' in datastream_res.keys():
            lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
        else:
            lastEntryTime = None

        print(f"Processing Thing ID: {id_}, Start: {datetime.now()}, Import: {lastEntryTime}, Datastream: {datastream}")
        # # !!! FOR DEV ONLY !!!
        if foreign_id == '14828010':

            url = f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/station/{foreign_id}/datenspuren/parameter/{PAT_ID}/tage/{tage}?key={api_key}' 
            
            measurements_res = session.get(url)
            if measurements_res.status_code == 200:
                # create DataFrame
                df = pd.DataFrame(measurements_res.json())
                #df['getPegelDatenspurenResult']['Parameter'][0]['Datenspuren'][0]['Pegelstaende']
                # df.rename(columns={'timestamp': 'phenomenonTime', 'value': 'result'}, inplace=True)

                # if lastEntryTime:
                #     df_to_post = df[df['phenomenonTime'] > lastEntryTime][-5:]
                # else:
                #     df_to_post = df[-5:]
                
                # post_observations(session, datastream, df_to_post)
            else:
                print(f'Something went wrong! Status{measurements_res.status_code} - {measurements_res.text}')