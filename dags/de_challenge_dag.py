
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator

from src.check_modules import f_check_main, f_check_02_main, f_check_03_main, \
    branch_check, simulated_task
from src.trusted_module import f_trusted_main
from src.refined_modules import f_refined_main
from src.r0_modules import f_r0_main


default_args = {
    'owner': 'Airflow',
    'email': ['your.email@domain.com'],
    'start_date': days_ago(1),
    'email_on_failure' : False
}

with DAG(
    dag_id="de-challenge_covid19_dag_radix",
    default_args=default_args,
    # Intervalo de tempo que nossa orquestração irá rodar
    schedule_interval='@daily',
    catchup=False,
) as dag:
    

    check_task = PythonOperator(
        task_id = "check_task_ingestion",
        python_callable = f_check_main,
        provide_context=True,
    )

    branch_task = BranchPythonOperator(
        task_id = "branch_task",
        python_callable = branch_check,
        provide_context=True,
    )

    simlatuon_task = PythonOperator(
        task_id = "simulated_action",
        python_callable = simulated_task
    )

    trusted_task = PythonOperator(
        task_id = "trusted_task",
        python_callable = f_trusted_main
    )

    check_parquet_trusted = PythonOperator(
        task_id = "check_task_trusted",
        python_callable = f_check_02_main
    )

    refined_task = PythonOperator(
        task_id = "refined_task",
        python_callable = f_refined_main
    )

    check_parquet_refined = PythonOperator(
        task_id = "check_task_refined",
        python_callable = f_check_03_main
    )

    r0_task = PythonOperator(
        task_id = "r0_task",
        python_callable = f_r0_main
    )


check_task >> branch_task
branch_task >> [trusted_task, simlatuon_task]
trusted_task >> check_parquet_trusted >> refined_task >> check_parquet_refined >> r0_task