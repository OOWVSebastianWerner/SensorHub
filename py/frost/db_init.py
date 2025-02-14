#------------------------------------------------------------------------------
# init FROST-Server DB
#------------------------------------------------------------------------------
#%%
import requests
import json
from config import endpoints

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
        "description": "Water level",
        "properties": {},
        "definition": ""
    }
]

#%%
with requests.Session() as s:

    for i in Sensors:
        # check if entry already exists
        f = s.get(f"{endpoints['sensors']}?$filter=name eq '{i['name']}'").json()
        if not f['value']:
            r = s.post(f'{endpoints['sensors']}', json.dumps(i, default=str))
            print(r.text)


    for j in ObservedProperties:
        check_if_exists = s.get(f"{endpoints['observedProperties']}?$filter=name eq '{j['name']}'").json()
        if not check_if_exists['value']:
            r = s.post(f'{endpoints['observedProperties']}', json.dumps(j, default=str))
            print(r.text)
# %%
