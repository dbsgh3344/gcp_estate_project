from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import datetime

default_dag_args = {
      'owner': 'song',
      'start_date': datetime.datetime(2023, 4, 21),
      'email': ['dbsgh3322@gmail.com'],
      'email_on_failure': False,
      'email_on_retry': False,
      'retries': 0,
      'project_id': 'estate-project-382208'
  }

query = """create table tmp.test (id int64 not null,name string,number int64);"""

with models.DAG (
dag_id = 'bq_test',
schedule_interval = '@once',
default_args = default_dag_args
) as dag :
    bq_query = BigQueryOperator (
        task_id = 'bq_access',
        sql = query,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id = 'bigquery_default'
    )
    bq_query