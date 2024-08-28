from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import dropbox
import io

DROPBOX_ACCESS_TOKEN = Variable.get('DROPBOX_ACCESS_TOKEN')

TAGS = ['PythonDataFlow']
DAG_ID = "covid19_ETL_orchestration"
DAG_DESCRIPTION = """ETL_ORCHESTRATION_FROM_COVID19_DATA"""
DAG_SCHEDULE = "0 9 * * *"
default_args = {
    "start_date": datetime(2024, 8, 28),
}
retries = 5
retry_delay = timedelta(minutes=15)

## FUNCTIONS STATEMENT

def execute_data_lake_upload_task():
    dbx = dropbox.Dropbox("################## variable de airflow ###################")

    data_df = pd.read_csv("https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv")
    data_buffer = io.StringIO()
    data_df.to_csv(data_buffer, index=False)
    csv_data = data_buffer.getvalue()

    dbx.files_upload(
        csv_data.encode('utf-8'),
        '/us.csv',
        mode=dropbox.files.WriteMode('overwrite'))
    
def execute_data_transform_task():
    data_df = pd.read_csv("https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv")

dag=DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    catchup=False,
    schedule_interval=DAG_SCHEDULE,
    max_active_runs=1,
    dagrun_timeout=timedelta(seconds=200000),
    default_args=default_args,
    tags=TAGS
)

with dag as dag:
    start_task = EmptyOperator(
        task_id="Proccess_start"
    )

    end_task = EmptyOperator(
        task_id="Proccess_ended"
    )

    data_lake_upload_task = PythonOperator(
        task_id="data_lake_upload_task",
        python_callable=execute_data_lake_upload_task,
        retries=retries,
        retry_delay=retry_delay
    )

start_task >> data_lake_upload_task >> end_task