import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime
import os

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id = "dag_test",
    description="test",
    start_date=datetime.datetime(2023,6,20,11,50,tzinfo=time_z),
    schedule_interval="0/5 * * * *",
)

def test():
    cur_path =os.path.dirname(os.path.realpath(__file__))
    curdate= str(datetime.datetime.now(tz=time_z))+ os.getcwd()
    with open(os.path.join(cur_path,'testlog.txt'),'a') as f:
        f.write(curdate+'\n')



get_p = PythonOperator(
    task_id= 'test_task',
    python_callable=test,
    queue='server04',
    dag=dag
)


# get_p


cur_path =os.path.dirname(os.path.realpath(__file__))

get_p
