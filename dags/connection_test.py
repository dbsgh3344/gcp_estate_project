import json
import pathlib
import pandas as pd
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG,macros
from airflow.providers.sftp.operators.sftp import SFTPOperator
import datetime
import pendulum

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "conntest",
    description= "crawling songdo apt info",
    start_date= datetime.datetime(2023,5,10,tzinfo= time_z),
    # schedule_interval= "@daily",
    # schedule_interval= "@hourly",
    schedule_interval="@once",
    catchup=False
)

tt = SFTPOperator(
    task_id = f'trans_data',
    ssh_conn_id = f'ssh_worker_1',
    local_filepath = "/opt/airflow/test.txt",
    remote_filepath = f"/home/dbsgh3322/estate_project/dags/test.txt",
    operation = "put",
    queue = 'server04',
    dag=dag)

tt
