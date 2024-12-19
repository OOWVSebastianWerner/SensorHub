#------------------------------------------------------------------------------
# Name:         WSA get stations
#               Loads stations from PEGELONLINE API.
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
            with open(basePath / 'pegel_stations.json', 'w') as f:
                f.write(json.dumps(response.json(), indent=4))
            
            # data = response.json()

            # df_stations = pd.DataFrame(data)
            # # split column water into two columns
            # df_waters = pd.DataFrame(df_stations.water.to_list())
            # df_waters.rename(columns={'shortname': 'water_shortname', 'longname': 'water_longname'}, inplace=True)
            # df_stations = pd.concat([df_stations, df_waters], axis=1)
            # df_stations.drop(['water'], axis=1, inplace=True)

            # # Export df to csv
            # df_stations.to_json(basePath / 'pegel_stations.json', orient='records', indent=4)

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')
# %%
