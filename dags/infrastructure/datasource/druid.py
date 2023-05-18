import json
import re, os
import logging
import string
from airflow import AirflowException
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator


def __create_druid_json_ingest_payload(payload_template_file: string, replace_values_dict: dict):
    ingestion_payload = ''
    with open(payload_template_file, 'r') as file:
        ingestion_payload = file.read().replace('\n', ' ')

    logging.info(f'The ingestion payload after reading: {ingestion_payload}')
    ingestion_payload = re.escape(ingestion_payload).replace('\\', '')

    for placeholder, newval in replace_values_dict.items():
        ingestion_payload = ingestion_payload.replace(placeholder, newval)

    logging.info(f'The ingestion_payload after reworking: {ingestion_payload}')

    return ingestion_payload  



def __create_druid_sql_ingest_payload(payload_template_file: string, replace_values_dict: dict, max_num_tasks: int = 1):

    ingestion_payload = __create_druid_json_ingest_payload(payload_template_file, replace_values_dict)
    logging.info(f"The replace values I got: {replace_values_dict}")
    logging.info(f'The ingestion_payload after reworking: {ingestion_payload}')

    return json.dumps({
        "query": ingestion_payload,
        "context": {
            "maxNumTasks": max_num_tasks
        }
    })
 

def __druid_ingestion_response_check(response, task_instance):

    if response.status_code is 200:
        json_response =response.json()
        task_status = json_response["status"]["status"]
        if task_status == "SUCCESS":
            return True

        if task_status == "FAILED" or task_status == "CANCELLED":
            raise AirflowException(f"The druid task did not succeed: {json_response}")

    return False;

   

 

def create_druid_ingest_payload_operator(task_id: string, is_json_payload: bool, dag, payload_template_file: string, replace_values_dict: dict, max_num_tasks: int = 1):
    if is_json_payload:
        return PythonOperator(
            task_id=task_id,
            provide_context=True,
            python_callable=__create_druid_json_ingest_payload,
            op_args=[payload_template_file, replace_values_dict],
            dag=dag
        )
    else:
        return PythonOperator(
            task_id=task_id,
            provide_context=True,
            python_callable=__create_druid_sql_ingest_payload,
            op_args=[payload_template_file, replace_values_dict, max_num_tasks],
            dag=dag
        )

 

def send_druid_sql_ingestion_payload_operator(task_id, http_conn_id, dag, prev_task_id: string):
    return SimpleHttpOperator(
        task_id=task_id,
        http_conn_id=http_conn_id,
        method='POST',
        endpoint="druid/v2/sql/task",
        data=f"{{{{ ti.xcom_pull(task_ids='{prev_task_id}', key='return_value') }}}}",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: True if response.status_code is 200 or 202 else False,
        response_filter=lambda response: response.json()["taskId"],
        log_response=True,
        dag=dag,
    )

 

def send_druid_json_ingestion_payload_operator(task_id, http_conn_id, dag, prev_task_id: string):
    return SimpleHttpOperator(
        task_id=task_id,
        http_conn_id=http_conn_id,
        method='POST',
        endpoint="druid/indexer/v1/task",
        data=f"{{{{ ti.xcom_pull(task_ids='{prev_task_id}', key='return_value') }}}}",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: True if response.status_code is 200 or 202 else False,
        response_filter=lambda response: response.json()["task"],
        log_response=True,
        dag=dag,
    )

 

def druid_ingest_finished_check_operator(task_id, prev_task_id, dag):
    return HttpSensor(
        task_id=task_id,
        http_conn_id="druid_router",
        endpoint=f"/druid/indexer/v1/task/{{{{ ti.xcom_pull(task_ids='{prev_task_id}', key='return_value') }}}}/status",
        headers={"Content-Type": "application/json"},
        request_params={},
        extra_options={"check_response": False},
        response_check=__druid_ingestion_response_check,
        poke_interval=15,
        dag=dag,
    )      