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
        "connectionId": "f808667a-5c39-4658-8297-136651d6079b"
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {token}"
    }
    response = requests.post(url, json=payload, headers=headers)
    print(response.text)

    


with DAG(
    dag_id="dag_api",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["Henry"],
) as dag:
    
    trigger_airbyte = PythonOperator(
    task_id = "trigger_airbyte_sync",
    python_callable = trigger_airbyte_sync,
    )

    run_glue_job_bronze = AwsGlueJobOperator(
    task_id='run_glue_job_broze_to_silver',
    job_name='bronze_to_silver_api',  
    region_name='us-east-2',          
    iam_role_name='GlueJobRole_PI_M3',      
    script_location='s3://aws-glue-assets-493834425520-us-east-2/scripts/bronze_to_silver_api.py',  
    create_job_kwargs={
        'GlueVersion': '5.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X'
    })

    run_glue_job_silver = AwsGlueJobOperator(
    task_id='run_glue_job_silver_to_gold_api',
    job_name='silver_to_gold_api',  
    region_name='us-east-2',          
    iam_role_name='GlueJobRole_PI_M3',      
    script_location='s3://aws-glue-assets-493834425520-us-east-2/scripts/silver_to_gold_api.py',  
    create_job_kwargs={
        'GlueVersion': '5.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X'
    })

    trigger_airbyte >> run_glue_job_bronze >> run_glue_job_silver

