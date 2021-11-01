from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 10, 31)
}

with DAG('parallel_execution_example', schedule_interval='@daily', 
	default_args = default_args, catchup=False) as dag:

    task1 = BashOperator(
		task_id='task1',
		bash_command='sleep 5'
	)

    task2 = BashOperator(
		task_id='task2',
		bash_command='sleep 5'
	)

    task3 = BashOperator(
		task_id='task3',
		bash_command='sleep 5'
	)

    task4 = BashOperator(
		task_id='task4',
		bash_command='sleep 5'
	)

    task1 >> [task2,task3] >> task4
