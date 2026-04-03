from airflow import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'capstone_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='financial_risk_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id='run_adf_pipeline',
        pipeline_name='pl_incremental_transactions',
        resource_group_name='rg-capstone2',
        factory_name='adfcapstone123',
        azure_data_factory_conn_id='azure_data_factory_default',
        wait_for_termination=True,
    )