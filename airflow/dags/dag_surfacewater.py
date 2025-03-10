import requests
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from frost import config

with DAG(
    dag_id='collect_wsa_water_levels',
    description='Collect water levels from wsa api',
    tags=['sensorHub', 'surfacewater'],
    schedule_interval='*/5 * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
) as surfacewater_dag:
    
    def check_frost_server():
        server_check = requests.get(config.baseURL)
        if server_check.status_code != 200:
            raise requests.exceptions.ConnectionError
        
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
            "?$filter=properties/station_type eq 'water_station'"
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
    
    def load_water_levels(task_instance):
        things = task_instance.xcom_pull(key='things', task_ids='task_get_things')
        with requests.Session() as session:
            # Set global header
            session.headers.update({'Content-Type': 'application/json; charset=UTF-8'})
        
            curr_measurments = session.get(Variable.get('wsa_meas_url'))
        
            if curr_measurments.status_code == 200 and curr_measurments.json():
                # create DataFrame
                df = pd.DataFrame(curr_measurments.json())
                df_meas = df[['uuid','timeseries']]
                df_meas.loc[:,'result'] = df_meas['timeseries'].apply(lambda x: x[0].get('currentMeasurement').get('value'))
                df_meas.loc[:,'phenomenonTime'] = df_meas['timeseries'].apply(lambda x: x[0].get('currentMeasurement').get('timestamp'))


            for thing in things['value']:
                id_ = thing['@iot.id']
                uuid = thing['properties']['foreign_id']
                # IMPORTANT: only works when there is only one datastream per thing
                # @TODO: add filter/check to get the correct datastream if there is more than one.
                datastream = thing['Datastreams'][0]['@iot.selfLink']

                if not df_meas[df_meas['uuid'] == uuid].empty:
                    meas_dict = {
                        'phenomenonTime': df_meas[df_meas['uuid'] == uuid]['phenomenonTime'].iat[0],
                        'result': float(df_meas[df_meas['uuid'] == uuid]['result'].iat[0])
                    }
                    session.post(f'{datastream}/Observations', json=meas_dict)
    
    task_check_server = PythonOperator(
        task_id = 'task_check_server',
        python_callable=check_frost_server,
        dag=surfacewater_dag
    )

    # task: load_things
    task_get_things = PythonOperator(
        task_id = 'task_get_things',
        python_callable=load_things,
        dag=surfacewater_dag
    )
    task_get_waterlevels = PythonOperator(
        task_id = 'task_get_waterlevels',
        python_callable=load_water_levels,
        dag=surfacewater_dag
    )

    task_check_server >> task_get_things >> task_get_waterlevels