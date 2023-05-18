import json
import pathlib
import time
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
import datetime
from airflow.models import DagRun
from airflow.utils.state import State

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "tmp_test",
    description= "tttest",
    start_date= datetime.datetime(2023,5,15,tzinfo= time_z),
    schedule_interval= "2 20 * * *",
    # schedule_interval= "@hourly",
    # schedule_interval ='@once'
)

latest_dag_run = DagRun.find(dag_id="tmp_test", state=State.RUNNING)
if latest_dag_run:
    start_time = latest_dag_run[0].start_date
    end_time = latest_dag_run[0].end_date



def push_arg(**context) :
    dag_id = 'tmp_test'
    dag_runs = DagRun.find(dag_id=dag_id)
    args = None
    
    # raise ValueError('test')
    for dag_run in dag_runs:
        execution_date = dag_run.execution_date
        state = dag_run.state
        start_date = dag_run.start_date
        end_date = dag_run.end_date
        # print(f'dag_info {execution_date} {state} {start_date} {end_date}')
        if state == State.RUNNING : 
            args = start_date.strftime('%Y%m%d %H:%M')
            context['task_instance'].xcom_push(key='test',value = args)
    
    return args
            
    

def test(tt) :
    log_path = '/home/dbsgh3322/testlog.txt'
    # with open(log_path,'a') as f:
    #     f.write(f'success test dag st :{dag.start_date} end: {dag.schedule_interval} {dag.get_active_runs()} \n')
    dag_id = 'tmp_test'
    dag_runs = DagRun.find(dag_id=dag_id)
    
    
    # raise ValueError('test')
    for dag_run in dag_runs:
        execution_date = dag_run.execution_date
        state = dag_run.state
        start_date = dag_run.start_date
        end_date = dag_run.end_date
        print(f'dag_info {execution_date} {state} {start_date} {end_date}')

    # xp = context['task_instance'].xcom_pull(key='test')
    # print(f'xcom pull is {xp}')
    print(f'test : {tt}')

    time.sleep(10)   
    # print(f"DAG tmp_test was last successfully executed on {start_time} with an end time of {end_time}.")
    # print(f"execution date {DagRun.execution_date.desc()}")
    # else:
    #     print(f"DAG tmp_test has no successful runs.")

dag_id = 'tmp_test'
dag_runs = DagRun.find(dag_id=dag_id)


# raise ValueError('test')
for dag_run in dag_runs:
    execution_date = dag_run.execution_date
    state = dag_run.state
    start_date = dag_run.start_date
    end_date = dag_run.end_date
    print(f'externel dag_info {execution_date} {state} {start_date} {end_date}')

t = PythonOperator(
    task_id = 'upload_file',
    python_callable = test,
    # op_kwargs= "{{ti.xcom_pull(task_ids= 'push_arg',key='return_value')}}",
    dag = dag
)

xcom_push_task = PythonOperator(
    task_id = 'scom_push',
    python_callable = push_arg,
    dag= dag
)

xcom_push_task >> t