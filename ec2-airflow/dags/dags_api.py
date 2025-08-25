from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.bash import BashOperator
import os
from dotenv import load_dotenv
from datetime import datetime
import requests
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator


load_dotenv('/opt/airflow/.env')
client_id = os.getenv('CLIENT_ID') 
client_secret = os.getenv('CLIENT_SECRET')

def get_airbyte_token():
    print("Executing get_airbyte_token")
    payload = {
        "grant-type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    headers = {
    "accept": "application/json",
    "content-type": "application/json"
    }
    url = "https://api.airbyte.com/v1/applications/token"
    print(url)
    print(payload)
    print(headers)
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    token = response.json().get("access_token")
    return token



def trigger_airbyte_sync():
    print("Executing trigger_airbyte_sync")
    token = get_airbyte_token()
    url = "https://api.airbyte.com/v1/jobs"
    payload = {
        "jobType": "sync",
        "connectionId": "e4b6dbae-7d74-4b72-9678-dc5d8b0f3afc"
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {token}"
    }
    response = requests.post(url, json=payload, headers=headers)
    print(response.text)

    


with DAG(
    dag_id="dag_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["Henry"],
) as dag:
    
    task_test = BashOperator(
    task_id = "test",
    bash_command = "echo 1",
    )

    trigger_airbyte = PythonOperator(
    task_id = "trigger_airbyte_sync",
    python_callable = trigger_airbyte_sync,
    )

    run_glue_job = AwsGlueJobOperator(
    task_id='run_glue_job_broze_to_silver',
    job_name='bronze_to_silver',  # reemplazá con el nombre real del Glue Job
    region_name='us-east-2',          # o la región donde lo creaste
    iam_role_name='GlueJobRole_PI_M3',      # el rol IAM con permisos a S3
    script_location='s3://aws-glue-assets-493834425520-us-east-2/scripts/bronze_to_silver.py',  # si usás script en S3
    create_job_kwargs={
        'GlueVersion': '5.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X'
    })

    task_test >> trigger_airbyte



#trigger_airbyte_sync()