import requests
from datetime import datetime
import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from frost import config

with DAG(
    dag_id='collect_nlwkn_water_levels',
    description='Collect water levels from nlwkn api',
    tags=['sensorHub', 'groundwater'],
    schedule_interval='0 3 * * *', # every day at 03:00 (3:00 AM)
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
) as groundwater_dag:
   
    def post_observations(session, datastream, data):
        responses = []
        for _, row in data.iterrows():
            row_dict = row.to_dict()
            response = session.post(f'{datastream}/Observations', json=row_dict)
        responses.append((response.status_code, response.text))
        return responses

    def load_things(task_instance):
        qry_things = (
            f"{config.endpoints['things']}"
            "?$filter=properties/station_type eq 'groundwater_station'"
            "&$select=@iot.id,properties/foreign_id,properties/MS_MBP_mNHN"
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
    
    def load_water_levels(task_instance):
        PAT_ID = 536
        tage = -5
        utc = ZoneInfo('UTC')
        
        things = task_instance.xcom_pull(key='things', task_ids='task_get_things')
        with requests.Session() as session:
            for thing in things['value']:
                id_ = thing['@iot.id']
                foreign_id = thing['properties']['foreign_id']
                mbp = thing['properties']['MS_MBP_mNHN']
                datastream = thing['Datastreams'][0]['@iot.selfLink']

                # get last phenomenonTime in Datastream
                datastream_res = session.get(f'{datastream}').json()
                if 'phenomenonTime' in datastream_res.keys():
                    lastEntryTime = datastream_res['phenomenonTime'].split('/')[1]
                else:
                    lastEntryTime = None

                # print(f"Processing Thing ID: {id_}, Start: {datetime.now()}, Import: {lastEntryTime}, Datastream: {datastream}")

                url = f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/station/{foreign_id}/datenspuren/parameter/{PAT_ID}/tage/{tage}?key={Variable.get('nlwkn_api_key')}' 
                
                measurements_res = session.get(url)
                if measurements_res.status_code == 200:
                    # create DataFrame
                    meas = measurements_res.json()\
                        .get('getPegelDatenspurenResult')\
                            .get('Parameter')[0]\
                                .get('Datenspuren')[0]\
                                    .get('Pegelstaende')
                    
                    df = pd.DataFrame(meas)
                    df = df.replace(-777, np.nan)
                    df = df.dropna()

                    df['DatumUTC'] = df['DatumUTC'].apply(lambda x: datetime.fromtimestamp(int(x.split('/Date(')[-1][0:-2])/1000, tz=utc))
                    df.rename(columns={'DatumUTC': 'phenomenonTime', 'Wert': 'result'}, inplace=True)
                    df = df.drop(['Datum', 'Grundwasserstandsklasse'], axis=1)
                    df['phenomenonTime'] = df['phenomenonTime'].apply(lambda x: x.isoformat())
                    df['result'] = df['result'].apply(lambda x: mbp - x)
                    df['result'] = df['result'].round(2)

                    if lastEntryTime:
                        df_to_post = df[df['phenomenonTime'] > lastEntryTime][-5:]
                    else:
                        df_to_post = df[-5:]

                    if not df_to_post.empty:
                        res = post_observations(session, datastream, df_to_post)
                        # print(res)
    
    # task: load_things
    task_get_things = PythonOperator(
        task_id = 'task_get_things',
        python_callable=load_things,
        dag=groundwater_dag
    )
    task_get_waterlevels = PythonOperator(
        task_id = 'task_get_waterlevels',
        python_callable=load_water_levels,
        dag=groundwater_dag
    )

    task_get_things >> task_get_waterlevels