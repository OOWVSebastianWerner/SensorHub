#------------------------------------------------------------------------------
# Name:         WSA get stations
#               Loads stations and waterlevels (last 30 days) from PEGELONLINE 
#               API.
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      13.08.2024
#------------------------------------------------------------------------------
#%%
import requests
import json
import pandas as pd
from pathlib import Path
import os

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%

basePath = Path(r'..\data\wsa')

levelsPath = Path(basePath / 'levels')
if not levelsPath.exists():
    os.mkdir(levelsPath)

baseUrl = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/'
url = f'{baseUrl}stations.json'

#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------
try:
    with requests.Session() as s:
    
        s.headers.update({'content-type': 'application/json; charset=UTF-8'})
        
        response = s.get(url)

        if response.status_code == 200:
            data = response.json()

            df_stations = pd.DataFrame(data)
            # split column water into two columns
            df_waters = pd.DataFrame(df_stations.water.to_list())
            df_waters.rename(columns={'shortname': 'water_shortname', 'longname': 'water_longname'}, inplace=True)
            df_stations = pd.concat([df_stations, df_waters], axis=1)
            df_stations.drop(['water'], axis=1, inplace=True)

            # Export df to csv
            df_stations.to_json(basePath / 'pegel_stations.json', orient='records', indent=4)
        
        # Limit for water HUNTE for testing
        ls_uuids = df_stations[df_stations['water_shortname'] == 'HUNTE'].uuid.to_list()

        for uuid in ls_uuids:
            url_measurements = f'{baseUrl}stations/{uuid}/W/measurements.json'
            r = s.get(url_measurements)

            if r.status_code == 200:
                data = r.json()

                df = pd.DataFrame(data)
                df['stationId'] = uuid

                df.to_csv(levelsPath / f'{uuid}.csv', sep=';')
                
                # with open(basePath / 'pegel' / f'{uuid}.json', 'w') as f:
                #     json.dump(r.json(), f, ensure_ascii=False)

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')
# %%
