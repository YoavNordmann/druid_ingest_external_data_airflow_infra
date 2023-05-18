import string
import logging
import re
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator


def __contruct_sql_from_template(sql_template_file: string, replace_values_dict: dict):
    sql_string = ''
    with open(sql_template_file, 'r') as file:
        sql_string = file.read().replace('\n', ' ')

    logging.info(f'The sql string after reading: {sql_string}')
    sql_string = re.escape(sql_string).replace('\\', '')

    for placeholder, newval in replace_values_dict.items():
        sql_string = sql_string.replace(placeholder, newval)

    logging.info(f'The sql string after reworking: {sql_string}')
    return sql_string  

 
def create_sql_string(task_id: string, dag: DAG, sql_template_file: string, replace_values_dict: dict):
    return PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=__contruct_sql_from_template,
        op_args=[sql_template_file, replace_values_dict],
        dag=dag
    )


def create_snowflake_operator_task(task_id: string, prev_task_id: string):
    return SnowflakeOperator(
        task_id=task_id,
        sql=f"{{{{ ti.xcom_pull(task_ids='{prev_task_id}', key='return_value') }}}}"
    )