# Library importing

from datetime import timedelta
# for instantitating a DAG
from airflow import DAG 
# for incorporating bash script in task writing 
from airflow.operators.bash_operator import BashOperator
# for scheduling
from airflow.utils.dates import days_ago

# DAG rguments

default_args = {
    'owner': 'Ramesh Sannareddy',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

# DAG definition 
dag = DAG (
    'dag_name',
    default_args = default_args,
    description = 'dag description',
    schedule_interval = timedelta(days=1),

)

# Task definition
# define the task 1 as node 1
extract = BashOperator(
    task_id = 'extract',
    bash_command = 'cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag = dag,
)

# define the task 2 as node 2
transform_and_load = BashOperator(
    task_id = 'transform_load_atthesametask',
    bash_command = 'tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag = dag,
)

# Task pipeline
# define the dependcies as the edges between nodes 
extract >> transform_and_load