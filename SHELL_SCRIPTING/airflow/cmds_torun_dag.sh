
#!/bin/bash

echo "Hello, $USER. 

This bash shell is going to run commnads to: 
run selected Airflow Dag script in Airflow server"

echo "listing files in the current directory, $PWD"

ls  


# Start the Airflow webserver. Please manually visit the Airflow UI on browser. 
airflow webserver -p 8080 

# Submit the DAG python script to the Airflow webserver
airflow dags trigger ETL_toll_data

# Check for detailed errors if the submission of DAG Python script is unsuccessful 
airflow dags list-import-errors

# Check if the submiited DAG Python script is on the Airflow's job list
airflow dags list

