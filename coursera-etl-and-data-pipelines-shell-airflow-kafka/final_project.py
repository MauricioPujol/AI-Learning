# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

default_args = {
    'owner': 'Mauricio Pujol',
    'start_date': days_ago(0),
    'email': ['mauriciopujol@latam.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks
# define the task 'unzip_data'
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command="sudo tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz",
    dag=dag,
)

# define the task 'extract_data_from_csv'
extract_data_from_csv = BashOperator( # Extract Rowid, Timestamp, Anonymized Vehicle number, Vehicle type
    task_id='extract_data_from_csv',
    bash_command="sudo cut -d',' -f1,2,3,4 vehicle-data.csv | tr -d '\t' > csv_data.csv",
    dag=dag,
)

# define the task 'extract_data_from_tsv'
extract_data_from_tsv = BashOperator( # Extract Number of axles, Tollplaza id, Tollplaza code
    task_id='extract_data_from_tsv',
    bash_command="sudo cut -d$'\t' -f5,6,7 tollplaza-data.tsv | sed 's/\t/,/g' > tsv_data.csv",
    dag=dag,
)

# define the task 'extract_data_from_fixed_width'
extract_data_from_fixed_width = BashOperator( # Type of Payment code, Tollplaza id, Vehicle code
    task_id='extract_data_from_fixed_width',
    bash_command="sudo tr -s ' ' ',' < payment-data.txt | cut -d',' -f11,12 > fixed_width_data.csv",
    dag=dag,
)

# define the task 'consolidate_data'
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="sudo paste -d, fixed_width_data.csv csv_data.csv tsv_data.csv > extracted_data.csv",
    dag=dag,
)

# define the task 'transform_data'
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="paste -d, <(cut -d',' -f1-3 extracted_data.csv) <(cut -d',' -f4 extracted_data.csv | tr '[:lower:]' '[:upper:]') <(cut -d',' -f5-8 extracted_data.csv) > transformed_data.csv",
    dag=dag,
)

unzip_data >>extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data