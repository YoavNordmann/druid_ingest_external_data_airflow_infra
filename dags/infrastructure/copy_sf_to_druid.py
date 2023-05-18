import os
import string
from airflow import DAG
from dags.infrastructure.datasource import druid_infra, snowflake_infra, azure


def copy_table_task(project_name: string, table_name: string, dag: DAG, table_task_spec: dict):
    dags_folder = os.path.join(os.getenv('AIRFLOW_HOME'), 'dags')
 
    snowflake_sql_template_file = os.path.join(dags_folder, f"dags/resources/snowflake_export/{project_name}/{table_name}.sql")
    druid_ingestion_template_file = os.path.join(dags_folder, f"dags/resources/druid_ingestion/{project_name}/{table_name}.sql")
    task_id_prefix = f"{project_name}_{table_name}"
    replace_dict = table_task_spec['replace_values']

    sf_sql_generation = snowflake_infra.create_sql_string(f"{task_id_prefix}_snowflake_sql_generation", dag,
                                                          snowflake_sql_template_file, replace_dict)

    export = snowflake_infra.create_snowflake_operator_task(
        f"{task_id_prefix}_snowflake_export", sf_sql_generation.task_id)

    druid_payload_generation = druid_infra.create_druid_ingest_payload_operator(f"{task_id_prefix}_druid_ingest_payload_generation",
                                                        False, dag, druid_ingestion_template_file,
                                                        replace_dict, table_task_spec['druid_task_num'])

    ingest = druid_infra.send_druid_sql_ingestion_payload_operator(f"{task_id_prefix}_druid_ingest",
                                                            "druid_router", dag, druid_payload_generation.task_id)

    test = druid_infra.druid_ingest_finished_check_operator(f"{task_id_prefix}_druid_ingest_check", ingest.task_id, dag)

    sf_sql_generation >> export >> druid_payload_generation >> ingest >> test

