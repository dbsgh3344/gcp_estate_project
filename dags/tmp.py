import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
import datetime

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "tmp_test",
    description= "tttest",
    start_date= datetime.datetime(2023,5,8,tzinfo= time_z),
    schedule_interval= "0 22 * * *",
    # schedule_interval= "@hourly",
)

def test() :
    log_path = '/home/dbsgh3322/testlog.txt'
    with open(log_path,'a') as f:
        f.write(f'success test dag st :{dag.start_date} end: {dag.end_date} \n')


t = PythonOperator(
    task_id = 'upload_file',
    python_callable = test,
    dag = dag
)

t