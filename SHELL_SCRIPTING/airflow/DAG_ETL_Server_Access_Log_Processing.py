from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Ramesh Sannareddy',
    'start_date': days_ago(0), # set the when the job firstly starts running
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG (
    'ETL_Server_Access_Log_Processing',
    default_args = default_args,
    description = 'dag description',
    schedule_interval = timedelta(days=1),
)

task1 = BashOperator (
    task_id = 'download',
    bash_command = 'wget -q "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" -O /home/project/downloadeddata.txt',
    dag = dag,
)

task2 = BashOperator (
    task_id = 'extract',
    bash_command = 'cut -d"#" -f1,4 /home/project/downloadeddata.txt > /home/project/airflow/dags/extracted.txt',
    dag = dag,
)

task3 = BashOperator (
    task_id = 'transform',
    bash_command = 'awk -F "#" "{print $1, tolower($2)}" /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/uncapitalized.txt',
    dag = dag,
)

task4 = BashOperator (
    task_id = 'load',
    bash_command = 'zip log.zip uncapitalized.txt',
    dag = dag,
)

task1 >> task2 >> task3 >> task4