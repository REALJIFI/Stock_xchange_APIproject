# importing libraries
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator





default_args ={
    'owner': 'George',
    'start_date': datetime(year =2024, month =11, day =13),
    'email': 'ifigeorgeifi@yahoo.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    #'retries_delay': None
}

# instatiate the DAG
with DAG(
    'my_first_dag',
    default_args = default_args,
    description = ' this is a pipeline to move alpah_vintage stock data to snowflake DB',
    schedule_interval = '0 0 * * 2,3', # Runs every Tuesday and Wednesday at midnight(check contrab guru)
    catchup = False
) as dag:

#define task
    start_task = EmptyOperator(
    task_id = 'start_pipeline'
)

#define task
end_task = EmptyOperator(
    task_id = 'end_pipeline'
)

# set dependencies
start_task >> end_task

