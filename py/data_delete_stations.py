#------------------------------------------------------------------------------
# Name:         data importer nlwkn stations
#               Imports the nlwkn stations into the FROST-Server
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      04.02.2025
#------------------------------------------------------------------------------
#%%
import os
from dotenv import load_dotenv
import requests
from frost import config
from frost import func
from tqdm import tqdm

#------------------------------------------------------------------------------
#--- global
#------------------------------------------------------------------------------

load_dotenv(r'..\.env')

filter = "properties/station_type eq 'groundwater_station'"
qry_things_to_delete = f"{config.endpoints['things']}?$select=@iot.id&$filter={filter}"
#------------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------------
#%%
try:
    with requests.Session() as session:
    
        session.headers.update({'content-type': 'application/json; charset=UTF-8'})
        session.headers.update({'Accept': 'application/json'})

        # get things from FROST-Server
        things = session.get(qry_things_to_delete).json()
        
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
        
        for thing in things['value']:
            print(thing['@iot.id'])
            response = session.delete(f'{config.endpoints['things']}({thing['@iot.id']})')
            print(response.text)
            
        

except requests.exceptions.HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except requests.exceptions.ConnectionError as conn_err:
    print(f'Connection error occurred: {conn_err}')
except requests.exceptions.Timeout as timeout_err:
    print(f'Timeout error occurred: {timeout_err}')
except requests.exceptions.RequestException as req_err:
    print(f'An error occurred: {req_err}')
# %%
