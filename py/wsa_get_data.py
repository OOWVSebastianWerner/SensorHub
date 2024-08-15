#------------------------------------------------------------------------------
# Name:         WSA get stations
# Purpose:      Loads stations and measurements from PEGELONLINE and saves the 
#               stations as csv and measurement data as json
#
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

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%

basePath = Path(r'data')
stationsPath = basePath / 'stations'

baseUrl = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/'

#------------------------------------------------------------------------------
#--- functions
#------------------------------------------------------------------------------
#%%
# get json data from endpoint
def get_data_as_json(url):
    
    res = requests.get(url, stream=True)
    res.raise_for_status()

    return res.json()

# Get stations
#%%
def get_wsa_stations():
    url = f'{baseUrl}stations.json'
    json_stations = get_data_as_json(url)

    # Convert json result into DataFrame
    df_stations = pd.DataFrame(json_stations)
    # split column water into two columns
    df_waters = pd.DataFrame(df_stations.water.to_list())
    df_waters.rename(columns={'shortname': 'water_shortname', 'longname': 'water_longname'}, inplace=True)
    df_stations = pd.concat([df_stations, df_waters], axis=1)
    df_stations.drop(['water'], axis=1, inplace=True)

    # Export df to csv
    df_stations.to_csv(basePath / 'pegel_stations.csv', sep=';')

    return df_stations

# Get measurements 
# for testing: limit to water Hunte
# %%
def get_wsa_measurements(df, limit = None):
    
    if limit:
        ls_uuids = df[df.where(df[limit['col']] == limit['value']).uuid.notna()].uuid.to_list()
    else:
        ls_uuids = df.uuid.to_list()

    for uuid in ls_uuids:
        url_measurements = f'{baseUrl}stations/{uuid}/W/measurements.json'
        j = get_data_as_json(url_measurements)
        with open(stationsPath / f'{uuid}.json', 'w') as f:
            json.dump(j, f, ensure_ascii=False)

if __name__ == 'main':
    df_stations = get_wsa_stations()
    get_wsa_measurements(df_stations)
