#------------------------------------------------------------------------------
# init FROST-Server DB
#------------------------------------------------------------------------------
#%%
import requests
import json
from frost_config import baseURL, endpoints

Sensors = [
    {
        "name":"Unknown Sensor",
        "description":"Placeholder, if the sensor of the thing is unknown.",
        "encodingType":"application/text",
        "metadata":"unknown"
    },
]

ObservedProperties = [
    {
        "name": "Temperature",
        "description": "Temperature",
        "properties": {},
        "definition": "http://dd.eionet.europa.eu/vocabularyconcept/aq/meteoparameter/54"
    },{
        "name": "Precipitation",
        "description": "Precipitation",
        "properties": {},
        "definition": "http://dd.eionet.europa.eu/vocabulary/aq/meteoparameter/60"
    },{
        "name": "Water level",
        "description": "Water level in meter NHN.",
        "properties": {},
        "definition": ""
    }
]

#%%
with requests.Session() as s:

    for i in Sensors:
        r = s.post(f'{baseURL}{endpoints['sensors']}', json.dump(i, default=str))
        print(r.text)

    #%%
    for j in ObservedProperties:
        r = s.post(f'{baseURL}{endpoints['observedProperties']}', json.dump(i, default=str))
        print(r.text)