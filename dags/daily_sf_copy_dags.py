from datetime import timedelta
from datetime import datetime
from airflow import DAG
from dags.infrastructure.copy_snowflake_table import copy_table_task

 

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['yoav.nordman@bnymellon.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'sla': timedelta(hours=1)
}
 
with DAG(
    'reporting_data_copy',
    max_active_runs=1,
    default_args=default_args,
    description='INgestion of data form snowflake',
    start_date=datetime(2023, 1, 1),
    tags=['reporting','druid', 'snowflake'],
    catchup=False
) as dag:

    replace_values = {
        "@YEAR": "{{ execution_date.strftime('%Y') }}",
        "@EXECUTION_TIMESTAMP":"{{ execution_date.strftime('%Y-%m-%dT%H:%M:%S.%z') }}"
    }


    copy_table_task('reporting','AWR_MGR_MODEL', dag, {'druid_task_num': 10, 'replace_values': replace_values})
    copy_table_task('reporting','AWR_FUND_PRICE', dag, {'druid_task_num': 10, 'replace_values': replace_values})
 