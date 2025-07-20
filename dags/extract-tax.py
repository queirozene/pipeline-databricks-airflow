from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'execute_notebook_etl',
    start_date=datetime(2025, 7, 19),
    schedule_interval="0 9 * * *",
    tags=["databricks", "etl"]
) as dag_execute_notebook_etl:
    
    extract_data = DatabricksRunNowOperator(
        task_id='extract-convert',
        databricks_conn_id='databricks_default',
        job_id=250655921749510,
        notebook_params={
            "data_execucao": "{{ data_interval_end.strftime('%Y-%m-%d') }}"
        }
    )

    transform_data = DatabricksRunNowOperator(
        task_id='transform-data',
        databricks_conn_id='databricks_default',
        job_id=148012334934192
    )

    send_report = DatabricksRunNowOperator(
        task_id='send-report',
        databricks_conn_id='databricks_default',
        job_id=899162490214985
    )
    
    extract_data >> transform_data >> send_report


