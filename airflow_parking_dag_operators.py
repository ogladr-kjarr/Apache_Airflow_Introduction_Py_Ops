import requests
import time
import datetime as dt
import pandas as pd
import json
import re
from os import listdir, remove
from os.path import isfile, join

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException

base_dir = '/Users/ok/airflow/parkingdata/'

def download_api_data(**kwargs):

    api_uri = 'https://api.tfl.gov.uk/Occupancy/CarPark'
    r = requests.get(api_uri)

    if r.status_code == 200:
        filepostfix = str(time.time()) + '.json'
        filename = join(kwargs['base_dir'], filepostfix)

        with open(filename, 'w') as f:
            f.write(r.text)
    else:
        raise AirflowException()
 
def api_json_to_csv(**kwargs):

    json_files = [f for f in listdir(kwargs['base_dir']) 
                    if isfile(join(kwargs['base_dir'], f)) 
                    if '.json' in f]

    csv_file = [f for f in listdir(kwargs['base_dir']) 
                    if isfile(join(kwargs['base_dir'], f)) 
                    if 'parking_data.csv' in f]

    df_columns = [
        'timestamp',
        'park_id',
        'park_name',
        'park_bay_type',
        'park_bay_occupied',
        'park_bay_free',
        'park_bay_count']

    parking_df = pd.DataFrame(columns=df_columns)

    for current_file in json_files:
        time_pattern = re.compile('[0-9]*')
        pattern_match = time_pattern.match(current_file)

        if pattern_match is not None:
            timestamp = pattern_match.group()

            with open(join(kwargs['base_dir'],current_file), 'r') as f:
                car_parks = json.loads(f.read())

            for car_park in car_parks:
                park_id = car_park['id']
                park_name = car_park['name']

                for bay in car_park['bays']:
                    park_bay_type = bay['bayType']
                    park_bay_occupied = bay['occupied']
                    park_bay_free = bay['free']
                    park_bay_count = bay['bayCount']

                    parking_df = parking_df.append({
                        'timestamp': timestamp,
                        'park_id': park_id,
                        'park_name': park_name,
                        'park_bay_type': park_bay_type,
                        'park_bay_occupied': park_bay_occupied,
                        'park_bay_free': park_bay_free,
                        'park_bay_count': park_bay_count
                    }, ignore_index=True)

    filepostfix = 'parking_data.csv'
    filename = join(kwargs['base_dir'], filepostfix)

    if len(csv_file) > 0:
        with open(filename, 'a') as f:
            parking_df.to_csv(f, 
                              header=False,
                              index=False)
    else:
        parking_df.to_csv(filename,
                          index=False)

    for current_file in json_files:
        remove(join(kwargs['base_dir'],current_file)) 
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2018, 10, 1, 10, 00, 00),
    'concurrency': 1,
    'retries': 0,
    'provide_context': True
}
 
with DAG('london_parking_bay_harvest_operators',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/15 * * * *',
         # schedule_interval=None,
         ) as dag:

    opr_download_data = PythonOperator(
        task_id='download_api_data',
        op_kwargs={'base_dir': base_dir},
        python_callable=download_api_data
    )
 
    api_json_to_csv = PythonOperator(
        task_id='api_json_to_csv',
        op_kwargs={'base_dir': base_dir},
        python_callable=api_json_to_csv
    )

opr_download_data.set_downstream(api_json_to_csv)
