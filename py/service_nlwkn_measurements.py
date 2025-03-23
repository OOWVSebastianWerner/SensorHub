#------------------------------------------------------------------------------
# Name:         service post nlwkn measurements to FROST-Server
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      16.01.2025
#------------------------------------------------------------------------------
#%%
import os
from dotenv import load_dotenv
import requests
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
from frost import config
from tqdm import tqdm

#------------------------------------------------------------------------------
#--- global 
#------------------------------------------------------------------------------
#%%

load_dotenv(r'..\.env')

api_key = os.getenv('NLWKN_API_KEY')
baseUrl_nlwkn = f'{os.getenv("NLWKN_STATIONS_URL")}?key={api_key}'

PAT_ID = 536
days = -30
utc = ZoneInfo('UTC')

qry_things = (
    f"{config.endpoints['things']}"
    "?$filter=properties/station_type eq 'groundwater_station'"
    "&$select=@iot.id,properties/foreign_id,properties/MS_MBP_mNHN"
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
try: 
    with requests.Session() as session:
        # Set global header
        session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})

        # get things from FROST-Server
        things = session.get(qry_things).json()
        
        # FROST-Server limits requests to 100 entries by default
        # as long as there is '@iot.nextLink' present in things, do another request 
        # and combine it with things
        while '@iot.nextLink' in things.keys():
            
            nextLink = things['@iot.nextLink'].replace('http://frost-server:8080', 'http://localhost:8080')
            next_things = session.get(nextLink).json()

            things['value'] += next_things['value']

            if '@iot.nextLink' in next_things.keys():
                things['@iot.nextLink'] = next_things['@iot.nextLink'].replace('http://frost-server:8080', 'http://localhost:8080')
            else:
                things.pop('@iot.nextLink')

        for thing in things['value']: #tqdm(things['value'], desc='Loading observations for station...', ascii=False, ncols=75):
            id_ = thing['@iot.id']
            foreign_id = thing['properties']['foreign_id']
            # mbp = thing['properties']['MS_MBP_mNHN']
            datastream = thing['Datastreams'][0]['@iot.selfLink'].replace('http://frost-server:8080', 'http://localhost:8080')

            # get last phenomenonTime in Datastream
            datastream_res = session.get(f'{datastream}').json()
            if 'phenomenonTime' in datastream_res.keys():
                lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
            else:
                lastEntryTime = None

            print(f"Processing Thing ID: {id_}, Start: {datetime.now()}, Import: {lastEntryTime}, Datastream: {datastream}")

            url = f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/station/{foreign_id}/datenspuren/parameter/{PAT_ID}/tage/{days}?key={api_key}' 
            
            measurements_res = session.get(url)
            if measurements_res.status_code == 200:
                # create DataFrame
                meas = measurements_res.json()\
                    .get('getPegelDatenspurenResult')\
                        .get('Parameter')[0]\
                            .get('Datenspuren')[0]\
                                .get('Pegelstaende')
                
                df = pd.DataFrame(meas)
                df = df.replace(-777, np.nan)
                df = df.dropna()

                df['DatumUTC'] = df['DatumUTC'].apply(lambda x: datetime.fromtimestamp(int(x.split('/Date(')[-1][0:-2])/1000, tz=utc))
                df.rename(columns={'DatumUTC': 'phenomenonTime', 'Wert': 'result'}, inplace=True)
                df = df.drop(['Datum', 'Grundwasserstandsklasse'], axis=1)
                df['phenomenonTime'] = df['phenomenonTime'].apply(lambda x: x.isoformat())
                # df['result'] = df['result'].apply(lambda x: mbp - x)
                # df['result'] = df['result'].round(2)

                if lastEntryTime:
                    df_to_post = df[df['phenomenonTime'] > lastEntryTime][days:]
                else:
                    df_to_post = df[days:]

                if not df_to_post.empty:
                    res = post_observations(session, datastream, df_to_post)
                    print(res)
            else:
                print(f'Something went wrong! Status{measurements_res.status_code} - {measurements_res.text}')

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')
# %%
