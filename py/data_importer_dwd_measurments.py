#%%
import requests
from pathlib import Path
import pandas as pd
from datetime import datetime
from frost import frost_config


#basePath = Path(r'..\data')
#levelsPath = Path(basePath / r'wsa\levels')

#baseUrl_wsa = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/'

qry_things = (
    f"{frost_config.baseURL}{frost_config.endpoints['things']}"
    "?$select=@iot.id,properties/uuid"
    "&$expand=Datastreams($select=@iot.id,@iot.selfLink)"
)
#%%
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

    for thing in things['value']:
        id_ = thing['@iot.id']
        uuid = thing['properties']['uuid']
        datastream = thing['Datastreams'][0]['@iot.selfLink']

        datastream_res = session.get(f'{datastream}').json()
        # get last phenomenonTime in Datastream
        if 'phenomenonTime' in datastream_res.keys():
            lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
        else:
            lastEntryTime = None

        
# %%





#------------------------------------------------------------------------------
# Name:         data importer dwd stations
#               Imports the dwd stations into the FROST-Server
#
# author:       Ivo Sieghold
# email:        i.sieghold@oowv.de
# created:      20.11.2024
#------------------------------------------------------------------------------
#%%
import requests
import pandas as pd
from pathlib import Path
import json

server = r'http://localhost:8080/FROST-Server/v1.1/'
basePath = Path(r'C:\Users\User\Desktop\stundendaten')
rows = []
file_path = r'C:\Users\User\Desktop\stundendaten\stations.txt'

with open(file_path, "r", encoding="latin1") as f:
    for line in f:
        # Zeile in Felder aufteilen
        parts = line.split()

        # Relevante Felder extrahieren
        if len(parts) >= 9:
            stations_id = parts[0]
            von_datum = parts[1]
            bis_datum = parts[2]
            stationshoehe = parts[3]
            geobreite = parts[4]
            geolaenge = parts[5]
            bundesland = parts[-2]
            abgabe = parts[-1]
            
            # Stationsname rekonstruiert aus allen Feldern zwischen `geolaenge` und `bundesland`
            stationsname = " ".join(parts[6:-2])
            
            rows.append([
                stations_id, von_datum, bis_datum, stationshoehe, geobreite, 
                geolaenge, stationsname, bundesland, abgabe
            ])


spaltennamen = [
     "Stations_id", "von_datum", "bis_datum", "Stationshoehe", "geoBreite", 
     "geoLaenge", "Stationsname", "Bundesland", "Abgabe"
]


df = pd.DataFrame(rows, columns=spaltennamen)
df= df.drop(0)


##%

for i in df:
    





















#%%
for i in df.index:
    station_dict = {
        'name': df.loc[i].Stationsname,
        'description': "",
        'properties': {
            'station type': "rain gauge",
            'uuid': df.loc[i].Stations_id,
            'Bundesland': df.loc[i].Bundesland,
            'Stationshöhe': df.loc[i].Stationshoehe,
            'von_Datum': df.loc[i].von_datum,
            'bis_Datum': df.loc[i].bis_datum,            
        },
        'Locations': [
            {
            'name': df.loc[i].Stationsname, 
            'description': "",
            'encodingType': "application/geo+json", 
            'location': { 
                'type': 'Point', 
                'coordinates': [df.loc[i].geoBreite, df.loc[i].geoLaenge]
                },
            }
        ],
        'Datastreams': [
            {
                'name': f"precipitation {df.loc[i].Stationsname}",
                'description': "",
                'observationType': "",
                "unitOfMeasurement": {
                    'name': 'millimeters',
                    'symbol': 'mm [m²]',
                    'definition': 'na'
                },
                'Sensor': {"@iot.id": 1},
                'ObservedProperty': {"@iot.id": 2

                }
            }
        ]
    }
    json_obj = json.dumps(station_dict, indent=4, default=str)
    
    r = requests.post(server + 'Things', json_obj)

    print(r.text)












# %%
