import json
import pathlib
import pandas as pd
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG,macros
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.python import PythonOperator
import datetime
import pendulum
import socket

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "conntest",
    description= "crawling songdo apt info",
    start_date= datetime.datetime(2023,6,10,tzinfo= time_z),
    # schedule_interval= "@daily",
    # schedule_interval= "@hourly",
    schedule_interval="@once",
    catchup=False
)
def _test():
    print("host name : ",socket.gethostname())


test = PythonOperator(
    task_id = 'testdata',
    python_callable =_test,
    queue = 'server04',
    dag=dag
)

tt = SFTPOperator(
    task_id = f'trans_data',
    ssh_conn_id = f'ssh_worker_1',
    local_filepath = "/opt/airflow/test.txt",
    remote_filepath = f"/home/dbsgh3322/estate_project/dags/test.txt",
    operation = "put",
    queue = 'server04',
    dag=dag)

# tt2 = SFTPOperator(
#     task_id = f'trans_data2',
#     ssh_conn_id = f'ssh_worker_3',
#     local_filepath = f"/home/dbsgh3322/estate_project/dags/test.txt",
#     remote_filepath = "/opt/airflow/test.txt",
#     operation = "get",
#     queue = 'server04',
#     dag=dag)

# tt2
test >> tt
# test >> tt2