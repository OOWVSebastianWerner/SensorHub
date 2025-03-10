import requests
from datetime import datetime
import pandas as pd
import numpy as np
import zipfile
import io
import os
import pytz
import csv

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from frost import config

output_dir = 'app/raw_files'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

with DAG(
    dag_id='collect_dwd_precipitation',
    description='Collect precipitation from dwd server',
    tags=['sensorHub', 'precipitation'],
    schedule_interval='0 8 * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
) as precipitation_dag:
   
    def post_observations(session, datastream, data):
        responses = []
        for _, row in data.iterrows():
            row_dict = row.to_dict()
            response = session.post(f'{datastream}/Observations', json=row_dict)
        responses.append((response.status_code, response.text))
        return responses

    def get_dwd_stations_with_new_data(task_instance):
        dwd_stations_url = Variable.get('dwd_stations_url')
        with requests.session() as session:
            dwd_stations = session.get(dwd_stations_url)
            # lines from text-file into list
            lines = dwd_stations.content.decode('cp1252').split('\r\n')

            # create list of stations that have data from yesterday
            dwd_station_ids = []
            # Date today in format yyyymmdd
            today = datetime.now().strftime('%Y%m%d')
            
            for line in lines:
                parts = line.split()
                
                if parts and parts[2] == today and parts[-1] == 'Frei':
                    # extract fields
                    stations_id = parts[0]

                    dwd_station_ids.append(int(stations_id))
        
        task_instance.xcom_push(key='dwd_station_ids', value=dwd_station_ids)

    def get_things(task_instance):
        qry_things = (
            f"{config.endpoints['things']}"
            "?$filter=properties/station_type eq 'raingauge_station'&$filter=properties/Abgabe eq 'Frei'"
            "&$select=@iot.id,properties/foreign_id"
            "&$expand=Datastreams($select=@iot.id,@iot.selfLink)"
        )
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
        # xcom einbauen
        task_instance.xcom_push(key='things', value=things)
        #return things
    
    def download_and_unzip_file(foreign_id):
        file_name = f"stundenwerte_RR_{str(foreign_id).zfill(5)}_akt.zip"
        url = Variable.get('dwd_baseURL') + file_name

        try:
            # print(f"Downloading {file_name} from {url}")  
            response = requests.get(url)  # Anfrage an den Zielserver
            response.raise_for_status()  

            # Zip-Datei in den Arbeitsspeicher laden
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall(os.path.join(output_dir, str(foreign_id).zfill(5)))
                print(f"Extracted {file_name} to {output_dir}/{foreign_id}")

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")  
        except Exception as err:
            print(f"Other error occurred: {err}")
    
    def load_precipitation_data(task_instance):
        
        things = task_instance.xcom_pull(key='things', task_ids='task_get_things')
        dwd_station_ids = task_instance.xcom_pull(key='dwd_station_ids', task_ids='task_get_dwd_stations')
       
        with requests.Session() as session:
            for thing in things['value']:
                # check if foreign-id in dwd_stations_ids to download
                if thing['properties']['foreign_id'] in dwd_station_ids:
                    id_ = thing['@iot.id']
                    uuid = thing['properties']['foreign_id']
                    # IMPORTANT: only works when there is only one datastream per thing
                    # @TODO: add filter/check to get the correct datastream if there is more than one.
                    datastream = thing['Datastreams'][0]['@iot.selfLink']

                    # get last phenomenonTime in Datastream
                    datastream_res = session.get(f'{datastream}').json()
                    if 'phenomenonTime' in datastream_res.keys():
                        lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
                    else:
                        lastEntryTime = None
                    
                    download_and_unzip_file(uuid)
                    
                    folder_path = os.path.join(output_dir,str(uuid).zfill(5))

                    txt_files = [f for f in os.listdir(folder_path) if f.startswith('produkt_rr_stunde') and f.endswith('.txt')]
                    
                    if txt_files:
                        # if more than one file matches e.g. because of historic data we'll take the newest (aplhabeticly sorted)
                        txt_files.sort()
                        txt_file_path = os.path.join(folder_path, txt_files[-1])  # Select last file
                        
                        json_data=[]

                        with open(txt_file_path, 'r', encoding='latin1') as file:
                            reader = csv.reader(file, delimiter=';')
                            rows = list(reader)

                            # Extract last 12 rows = last 12 values
                            for row in rows[-12:]:
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
                        
                        df = pd.DataFrame(json_data)

                        if lastEntryTime:
                            df_to_post = df[df['phenomenonTime'] > lastEntryTime]
                        else:
                            df_to_post = df
                        
                        if not df_to_post.empty:
                            res = post_observations(session, datastream, df_to_post)
                       
    # task: load_things
    task_get_things = PythonOperator(
        task_id = 'task_get_things',
        python_callable=get_things,
        dag=precipitation_dag
    )
    task_get_dwd_stations = PythonOperator(
        task_id = 'task_get_dwd_stations',
        python_callable=get_dwd_stations_with_new_data,
        dag=precipitation_dag
    )
    task_get_precipitation = PythonOperator(
        task_id = 'task_get_precipitation',
        python_callable=load_precipitation_data,
        dag=precipitation_dag
    )

    task_get_things >> task_get_precipitation
    task_get_dwd_stations >> task_get_precipitation