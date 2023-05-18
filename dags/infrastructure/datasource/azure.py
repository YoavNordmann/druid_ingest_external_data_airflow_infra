import string
from airflow import DAG
from airflow.operators.bash import BashOperator


def copy_from_blob_to_blob(task_id: string, dag: DAG, source_link: string, target_link: string):

    return BashOperator(
        task_id=task_id,
        bash_command=f"""curl -L https://aka.ms/downloadazcopy-v10-linux | \\
            tar --strip-components=1 --exclude=*.txt -xzvf -; \\
            chmod +x azcopy; \\
            ./azcopy copy "{source_link}" "{target_link}" --recursive""",
        dag=dag,
    )