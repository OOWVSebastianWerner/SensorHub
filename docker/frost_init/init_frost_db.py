#------------------------------------------------------------------------------
# init FROST-Server DB
#------------------------------------------------------------------------------
#%%
import requests
import json

baseURL = 'http://frost-server:8080/FROST-Server/v1.1/'

endpoints = {
    'things': f'{baseURL}Things',
    'locations': f'{baseURL}Locations',
    'sensors': f'{baseURL}Sensors',
    'observedProperties': f'{baseURL}ObservedProperties',
    'datastreams': f'{baseURL}Datastreams'
}

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
        sensor_exists = s.get(f"{endpoints['sensors']}?$filter=name eq '{i['name']}'").json()
        if not sensor_exists['value']:
            r = s.post(f"{endpoints['sensors']}", json.dumps(i, default=str))
            print(r.text)

    for j in ObservedProperties:
        ObservedProperty_exists = s.get(f"{endpoints['observedProperties']}?$filter=name eq '{j['name']}'").json()
        if not ObservedProperty_exists['value']:
            r = s.post(f"{endpoints['observedProperties']}", json.dumps(j, default=str))
            print(r.text)
# %%
