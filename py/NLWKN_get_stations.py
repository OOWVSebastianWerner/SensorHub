#------------------------------------------------------------------------------
# Name:         NLWKN get stations
# Purpose:      Lädt die Stationen (Messstellen) über di eAPI des 
#               NLWKN und speichert sie als json.
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

#------------------------------------------------------------------------------
#--- functions
#------------------------------------------------------------------------------
def get_data_as_json(url):
    
    res = requests.get(url, stream=True)
    res.raise_for_status()

    return res.json()

#------------------------------------------------------------------------------
#--- global vars
#%%
#------------------------------------------------------------------------------
# Public API Key
api_key = '9dc05f4e3b4a43a9988d747825b39f43'

url = f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/stammdaten/stationen/allegrundwasserstationen?key={api_key}'

json_stations = get_data_as_json(url)
# %%

df_stations = pd.DataFrame(json_stations['getStammdatenResult'])
with open('OOWVSebastianWerner/SensorHub/data/nlwkn_stations.json', 'r') as f:
    json.dump(json_stations, f, ensure_ascii=False, indent=4)
# %%
stationIds = 14880010

url_station = f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/stammdaten/stationen/{stationIds}?key={api_key}'

json_station = get_data_as_json(url_station)

with open('OOWVSebastianWerner/SensorHub/data/nlwkn_sensors.json') as f:
    json.dump(json_station, f, ensure_ascii=False, indent=4)
# %%
