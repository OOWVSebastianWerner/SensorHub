#------------------------------------------------------------------------------
# Name:         service post wsa measurements to FROST-Server
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      07.11.2024
#------------------------------------------------------------------------------
#%%
import requests
from pathlib import Path
import pandas as pd
from datetime import datetime
from frost import config
from tqdm import tqdm

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%

baseUrl_wsa = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/'

measURL_wsa = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json?includeTimeseries=true&includeCurrentMeasurement=true'

qry_things = (
    f"{config.endpoints['things']}"
    "?$filter=properties/station_type eq 'water_station'"
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
    # while '@iot.nextLink' in things.keys():
        
    #     nextLink = things['@iot.nextLink']
    #     next_things = session.get(nextLink).json()

    #     things['value'] += next_things['value']

    #     if '@iot.nextLink' in next_things.keys():
    #         things['@iot.nextLink'] = next_things['@iot.nextLink']
    #     else:
    #         things.pop('@iot.nextLink')

    curr_measurments = session.get(measURL_wsa)
        
    if curr_measurments.status_code == 200 and curr_measurments.json():
        # create DataFrame
        df = pd.DataFrame(curr_measurments.json())
        df_meas = df[['uuid','timeseries']]
        df_meas.loc[:,'result'] = df_meas['timeseries'].apply(lambda x: x[0].get('currentMeasurement').get('value'))
        df_meas.loc[:,'phenomenonTime'] = df_meas['timeseries'].apply(lambda x: x[0].get('currentMeasurement').get('timestamp'))


    for thing in tqdm(things['value'], desc='Loading observations for stations...', ascii=False, ncols=75):
        id_ = thing['@iot.id']
        uuid = thing['properties']['foreign_id']
        # IMPORTANT: only works when there is only one datastream per thing
        # @TODO: add filter/check to get the correct datastream if there is more than one.
        datastream = thing['Datastreams'][0]['@iot.selfLink']

        # datastream_res = session.get(f'{datastream}').json()
        # # get last phenomenonTime in Datastream
        # if 'phenomenonTime' in datastream_res.keys():
        #     lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
        # else:
        #     lastEntryTime = None
        if not df_meas[df_meas['uuid'] == uuid].empty:
            meas_dict = {
                'phenomenonTime': df_meas[df_meas['uuid'] == uuid]['phenomenonTime'].iat[0],
                'result': float(df_meas[df_meas['uuid'] == uuid]['result'].iat[0])
            }

            session.post(f'{datastream}/Observations', json=meas_dict)
        
            # df.rename(columns={'timestamp': 'phenomenonTime', 'value': 'result'}, inplace=True)

            # if lastEntryTime:
            #     df_to_post = df[df['phenomenonTime'] > lastEntryTime][-5:]
            # else:
            #     df_to_post = df[-5:]
            
            # r = post_observations(session, datastream, df_to_post)
            # print(r)

# %%
