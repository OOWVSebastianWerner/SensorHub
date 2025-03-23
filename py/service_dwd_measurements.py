
#%%
import os
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timezone
import csv
import pytz
import zipfile
import io
from frost import config

###############  DOWNLOAD of the .txt files from the DWD-Server ##################
#%%

load_dotenv(r'..\.env')

base_url = os.getenv('DWD_STATIONS_URL')
output_dir = os.getenv('TEMP')

## Get the staions for which we want to download data
#%%

qry_ids = (
    f"{config.endpoints['things']}"
    "?$filter=properties/station_type eq 'raingauge_station'"
    "&$select=@iot.id,properties/foreign_id"
)
with requests.Session() as session:
    # Set global header
    session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})

    # get things from FROST-Server
    things = session.get(qry_ids).json()
    
    # FROST-Server limits requests to 100 entries by default
    # as long as there is '@iot.nextLink' present in things, do another request 
    # and combine it with things
    while '@iot.nextLink' in things.keys():
        
        nextLink = things['@iot.nextLink']
        next_things = session.get(nextLink).json()

        things['value'] += next_things['value']

        if '@iot.nextLink' in next_things.keys():
            things['@iot.nextLink'] = next_things['@iot.nextLink']
        else:
            things.pop('@iot.nextLink')

#%%
def download_and_unzip_file(thing_id):
    file_name = f"stundenwerte_RR_{str(thing_id).zfill(5)}_akt.zip"
    url = base_url + file_name

    try:
        print(f"Downloading {file_name} from {url}")  #  Print der Anfrage
        response = requests.get(url)  # Anfrage an den Zielserver
        # Jede Antwort enthält einen Status-Code vom Server (z.B. 404)
        # die Funtkion wirft einen Fehler, sobald eine Antwort nicht den Code 200 (für alles ok) hat.
        response.raise_for_status()  # Überprüfe, ob der Download erfolgreich war

        # Zip-Datei in den Arbeitsspeicher laden
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(os.path.join(output_dir, str(thing_id).zfill(5)))
            print(f"Extracted {file_name} to {output_dir}/{thing_id}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP Fehler anzeigen
    except Exception as err:
        print(f"Other error occurred: {err}")  # Andere Fehler anzeigen

def main():
    for i in things['value']:
        download_and_unzip_file(i['properties']['foreign_id'])
        


if __name__ == "__main__":
   main()

#%%

############### IMPORT of the DATA to the Frost-Server  #######################

#### POST da data


qry_things = (
    f"{config.endpoints['things']}"
    "?$filter=properties/station_type eq 'raingauge_station'"
    "&$select=@iot.id,properties/foreign_id"
    "&$expand=Datastreams($select=@iot.id,@iot.selfLink)"
)

# POST-function
def post_observations(session, datastream, data):
    responses = []
    for _, row in data.iterrows():
        row_dict = row.to_dict()
        response = session.post(f'{datastream}/Observations', json=row_dict)
    responses.append((response.status_code, response.text))
    return responses

#%%

with requests.Session() as session:
    # Set global header
    session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})

    # get things from FROST-Server
    things = session.get(qry_things).json()
    
    # FROST-Server limits requests to 100 entries by default
    # as long as there is '@iot.nextLink' present in things, do another request 
    # and combine it with things
    while '@iot.nextLink' in things.keys():
        
        nextLink = things['@iot.nextLink']
        next_things = session.get(nextLink).json()

        things['value'] += next_things['value']

        if '@iot.nextLink' in next_things.keys():
            things['@iot.nextLink'] = next_things['@iot.nextLink']
        else:
            things.pop('@iot.nextLink')


    for thing in things['value']:
        id_ = thing['@iot.id']
        uuid = thing['properties']['foreign_id']
        # IMPORTANT: only works when there is only one datastream per thing
        # @TODO: add filter/check to get the correct datastream if there is more than one.
        datastream = thing['Datastreams'][0]['@iot.selfLink']

        datastream_res = session.get(f'{datastream}').json()
        # get last phenomenonTime in Datastream
        if 'phenomenonTime' in datastream_res.keys():
            lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
        else:
            lastEntryTime = None
##### Daten Holen

        folder_path = output_dir+"\\"+str(uuid).zfill(5)
        # Überprüfen, ob es ein Verzeichnis mit 5-stelliger Struktur ist

        print(f"Processing folder: {folder_path}")

        txt_files = [f for f in os.listdir(folder_path) if f.startswith('produkt_rr_stunde') and f.endswith('.txt')]

        if txt_files:
# Falls mehrere Dateien passen, z.B. bei historischen Daten, nehmen wir die neuste (nach Alphabet sortiert)
            txt_files.sort()
            txt_file_path = os.path.join(folder_path, txt_files[-1])  # Die letzte in der sortierten Liste
            print(f"Reading data from {txt_file_path}")

            # Datei öffnen und einlesen
            with open(txt_file_path, 'r', encoding='latin1') as file:
                reader = csv.reader(file, delimiter=';')
                rows = list(reader)

                # Header und Metadaten überspringen
                #data_rows = rows[:-20]
                
                # Nur die letzten 12 Zeilen
                last_12_rows = rows[-12:]

                print(f"Last 12 entries for station {folder_path}:")
                for row in last_12_rows:
                    print(row)
                else:
                    print(f"No .txt file found in {folder_path}")

                json_data=[]

                # Datum und Wert aus den letzten 12 Zeilen extrahieren und in JSON umwandeln
                for row in last_12_rows:
                    datum = row[1].strip()  # MESS_DATUM
                    wert = float(row[3].strip())   # RS
                    
                    datum_dt = datetime.strptime(datum, '%Y%m%d%H')

                    datum_utc = datum_dt.replace(tzinfo=pytz.UTC)
                    datum_iso = datum_utc.isoformat() 

                    # JSON-Format anpassen
                    json_entry = {
                        "phenomenonTime": datum_iso,
                        "result": wert
                    }
                    json_data.append(json_entry)

                #print(json_data)
                # Ausgabe der JSON-Daten
                #dataframe = json.dumps(json_data, indent=4)

                df = pd.DataFrame(json_data)
                #df['phenomenonTime']=df['phenomenonTime'].apply(lambda x: str(datetime.fromtimestamp(int(x), tz=utc)))
                #df['phenomenonTime']=df['phenomenonTime'].apply(lambda x: datetime.strptime(x, '%Y%m%d%H').replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'))
                #df['result']=df['result'].apply(lambda x: float(x.strip()))

                if lastEntryTime:
                    df_to_post = df[df['phenomenonTime'] > lastEntryTime][-5:]
                else:
                    df_to_post = df[-5:]
                
                
                r = post_observations(session, datastream, df_to_post)
                print(r)

# %%
