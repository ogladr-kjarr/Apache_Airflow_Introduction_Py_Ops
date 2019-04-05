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
from airflow.hooks import HttpHook, PostgresHook
from airflow.models import Variable

def download_api_data(**kwargs):

    tfl_hook = HttpHook(http_conn_id='tfl_parking_conn', method='GET')
    resp = tfl_hook.run('', extra_options={'check_response': True})

    filepostfix = str(time.time()) + '.json'
    base_dir = Variable.get("tfl_park_base_dir")
    filename = join(base_dir, filepostfix)

    with open(filename, 'w') as f:
        f.write(resp.text)
 
def api_json_to_db(**kwargs):
    base_dir = Variable.get("tfl_park_base_dir")

    json_files = [f for f in listdir(base_dir) 
                    if isfile(join(base_dir, f)) 
                    if '.json' in f]

    db_tuples = []

    for current_file in json_files:
        time_pattern = re.compile('[0-9]*')
        pattern_match = time_pattern.match(current_file)

        if pattern_match is not None:
            timestamp = pattern_match.group()

            with open(join(base_dir,current_file), 'r') as f:
                car_parks = json.loads(f.read())

            for car_park in car_parks:
                park_id = car_park['id']
                park_name = car_park['name']

                for bay in car_park['bays']:
                    park_bay_type = bay['bayType']
                    park_bay_occupied = bay['occupied']
                    park_bay_free = bay['free']
                    park_bay_count = bay['bayCount']

                    db_tuples.append((timestamp, 
                                        park_id, 
                                        park_name, 
                                        park_bay_type, 
                                        park_bay_occupied,
                                        park_bay_free,
                                        park_bay_count))

    target_fields = ['t_stamp',
                        'park_id',
                        'park_name',
                        'park_bay_type',
                        'park_bay_occupied',
                        'park_bay_free',
                        'park_bay_count']

    db_hook = PostgresHook(postgres_conn_id='tfl_db')
    db_hook.insert_rows('tfl_data',db_tuples, target_fields)

    for current_file in json_files:
        remove(join(base_dir,current_file))
 
 
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2018, 10, 1, 10, 00, 00),
    'concurrency': 1,
    'retries': 0,
    'provide_context': True
}
 
with DAG('london_parking_bay_harvest_hooks',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/15 * * * *',
         # schedule_interval=None,
         ) as dag:

    opr_download_data = PythonOperator(
        task_id='download_api_data',
        python_callable=download_api_data
    )
 
    api_json_to_db = PythonOperator(
        task_id='api_json_to_db',
        python_callable=api_json_to_db
    )

opr_download_data.set_downstream(api_json_to_db)
