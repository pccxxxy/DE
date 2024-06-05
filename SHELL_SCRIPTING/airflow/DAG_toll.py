from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, date, timedelta



# Define DAG arguments
default_dag = {
    "owner": "Felix Pratamasan",
    "start_date": date.today().isoformat(),
    "email": ["felixpratama242@gmail.com"],
    "email_on_failure": True,
    "email_on_entry": True,
    "retries":1,
    "retry_delay": timedelta(minutes=5),
    'catchup': False
}


# Define the DAG
# dag = DAG('ETL_toll_data',
#           schedule_interval= timedelta(days=1),
#           default_args= default_dag,
#           description="Apache Airflow Final Assignment"
#           )

with DAG(
    'ETL_toll_data',
    schedule_interval= '@daily',
    default_args= default_dag,
    description="Apache Airflow Final Assignment"
) as dag:

    # Tasks are implemented under the dag object
    unzip_data = BashOperator(
    task_id="task_1",
    bash_command="tar -xvzf tolldata.tgz --directory /home/project/airflow/dags/finalassignment/staging"
    # dag = dag
    )

    extract_data_from_csv = BashOperator(
    task_id="task_2",
    bash_command="cut -d ',' -f 1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv"
    # dag = dag
    )

    extract_data_from_tsv = BashOperator(
    task_id="task_3",
    bash_command="cut -d$'\t' -f 5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv"
    # dag = dag
    )

    extract_data_from_fixed_width = BashOperator(
    task_id="task_4",
    bash_command="cat /home/project/airflow/dags/finalassignment/staging/payment-data.txt | tr -s '[:space:]' | cut -d ' ' -f 11-12 > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv"
    # dag = dag
    )

    csv_data = "/home/project/airflow/dags/finalassignment/staging/csv_data.csv"
    tsv_data = "/home/project/airflow/dags/finalassignment/staging/tsv_data.csv"
    fixed_width_data = "/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv"
    extracted_data = "/home/project/airflow/dags/finalassignment/staging/extracted_data.csv"

    consolidate_data = BashOperator(
    task_id="task_5",
    bash_command=f"paste {csv_data} {tsv_data} {fixed_width_data} > {extracted_data}"
    # dag = dag
    )

    transform_data = BashOperator(
    task_id="task_6",
    bash_command = "awk 'BEGIN {FS=OFS=\",\"} { $4= toupper($4) } 1' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv"
    # dag = dag
    )

    # Define task pipeline
    # unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
    extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data