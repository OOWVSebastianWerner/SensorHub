#------------------------------------------------------------------------------
# Name:         NLWKN get stations
# Purpose:      Loads stations and current waterlevels from NLWKN API.
#
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      12.08.2024
#------------------------------------------------------------------------------
#%%
import requests
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re
import time

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%

basePath = Path(r'data\nlwkn')
# Public API Key
api_key = '9dc05f4e3b4a43a9988d747825b39f43'

url = f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/stammdaten/stationen/allegrundwasserstationen?key={api_key}'
#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------

try:
    with requests.Session() as s:
    
        #s.headers.update({'content-type': 'application/json; charset=UTF-8'})
        
        response = s.get(url)

        if response.status_code == 200:
            json_data = response.json()
            
            data = []

            for station in json_data['getStammdatenResult']:
                # extract station-id
                sta_id = station.get('STA_ID')
                # extract station name
                sta_name = station.get('Name')
                # extract current measurement
                curr_gw_value = station.get('GWAktuellerMesswert')
                # extract current measurement in NNM
                curr_gw_value_nnm = station.get('GWAktuellerMesswertNNM')
                curr_gw_class = station.get('AktuellGrundwasserstandsklasse')
                
                # extract date and convert to timestamp
                for parameter in station['Parameter']:
                    for datenspur in parameter['Datenspuren']:
                        if datenspur['AktuellerPegelstand']:
                            timestamp = datetime.fromtimestamp(
                                int(re.search(r'\d+', datenspur['AktuellerPegelstand']['Datum']).group()) / 1000 )
                        else:
                            timestamp = None

                data.append(
                    {
                        'timestamp': timestamp,
                        'STA_ID': sta_id,
                        'STA_Name': sta_name, 
                        'curr_gw_value': curr_gw_value,
                        'curr_gw_value_NNM': curr_gw_value_nnm,
                        'curr_gw_class': curr_gw_class
                    }
                )
        
        df = pd.DataFrame(data)

        df = df.replace(-888, np.nan)

        df.to_csv(basePath / 'nlwkn_data.csv', sep=';', decimal=',')

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')

# %%
