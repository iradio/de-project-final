from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.decorators import dag
from airflow.models import Variable

from datetime import datetime, timedelta
# import boto3

import vertica_python
import psycopg2

# import the logging module
import logging

import csv
import json

import os
import hashlib

# Don't forget to set up Connection and Variable in Airflow UI
# postgres_connection = 'postgres_connection'
postgres_connection = Variable.get("postgres_connection", deserialize_json=True)
vertica_connection = Variable.get("vertica_connection", deserialize_json=True)

batch_size = 1000

# get the airflow.task logger
logger = logging.getLogger("airflow.task")


def get_latest(connection, tablename, orderby, **context):
    # Establish a connection to the Vertica database
    conn = vertica_python.connect(**connection)

    # Create a cursor
    cur = conn.cursor()

    # Execute the query to select the latest value by the `date_update` column from the `currencies` table
    sql = f'SELECT * FROM TIM_ALEINIKOV_YANDEX_RU__STAGING."{tablename}" ORDER BY "{orderby}" DESC LIMIT 1'
    logger.info(f"SQL satement: {sql}")
    cur.execute(sql)
    
    # Fetch the result
    last_date = cur.fetchone() or '2022-10-01 00:00:00.000'
    # last_date = '2022-10-01 00:00:00.000'

    # Close the cursor and the connection
    cur.close()
    conn.close()
    
    context['ti'].xcom_push(key=f'last_{tablename}_{orderby}', value=last_date)
    return True

def extract_table(connection, tablename, orderby, **context):
    # Retrieve data from previous task
    last_date = context['ti'].xcom_pull(key=f'last_{tablename}_{orderby}')
    logger.info(f"Last date is {last_date}")

    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    logger.info(f"Execution date is {execution_date}")

    # conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    conn = psycopg2.connect(**connection)
    cur = conn.cursor()
    
    offset = 0
    rest = 1
    files = []
    while rest != 0 :

        # Execute the query to select all rows from the table
        sql = f"SELECT * FROM public.{tablename} WHERE to_char({orderby},'yyyy-mm-dd') ='{execution_date}' LIMIT {batch_size} OFFSET {offset}"
        logger.info(f"SQL satement: {sql}")
        cur.execute(sql)

        # Fetch all rows from the query result
        rows = cur.fetchall()
        rest = len(rows)
        logger.info(f"rows: {rest}")
        if rest == 0:
            break
        
        filepath = f"/tmp/airflow/stg_{tablename}_{execution_date}_{offset // batch_size}.csv"
        
        # Write the data to a CSV file
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([i[0] for i in cur.description])  # Write the header row
            for row in rows:
                writer.writerow(row)
            files.append(filepath)        

        # increment offset
        offset += batch_size

    # Close the cursor and database connection
    cur.close()
    conn.close()
    logger.info(f"Files: {filepath}")
    context['ti'].xcom_push(key=f'files_{tablename}_{execution_date}', value=json.dumps(files))


def load_table():
    return True

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

dag = DAG('final_project_stg', default_args=default_args, schedule_interval='@daily')


# Define the tasks in the first group
latest_currency_task = PythonOperator(
    task_id="latest_currency",
    python_callable=get_latest,
    op_kwargs={"connection": vertica_connection,
               "tablename":"currencies",
               "orderby": "date_update"
               },
    provide_context=True,
    dag=dag
)

extract_currencies_task = PythonOperator(
    task_id='extract_currencies',
    python_callable=extract_table,
    op_kwargs={"connection": postgres_connection,
               "tablename":"currencies",
               "orderby": "date_update"
               },
    provide_context=True,
    dag=dag
)

load_currencies_task = PythonOperator(
    task_id='load_currencies',
    python_callable=load_table,
    op_kwargs={"connection": vertica_connection,
               "tablename":"currencies",
               "orderby": "date_update"
               },
    provide_context=True,
    dag=dag
)

# Define the tasks in the second group
latest_transaction_task = PythonOperator(
    task_id="latest_transaction",
    python_callable=get_latest,
    op_kwargs={"connection": vertica_connection,
               "tablename":"transactions",
               "orderby": "transaction_dt"
               },
    provide_context=True,
    dag=dag
)

extract_transactions_task = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_table,
    op_kwargs={"connection": postgres_connection,
               "tablename":"transactions",
               "orderby": "transaction_dt"
               },
    provide_context=True,
    dag=dag
)

load_transactions_task = PythonOperator(
    task_id='load_transactions',
    python_callable=load_table,
    op_kwargs={"connection": vertica_connection,
               "tablename":"transactions",
               "orderby": "transaction_dt"
               },
    provide_context=True,
    dag=dag
)

# Define a dummy task to join the two groups
join_tasks = DummyOperator(
    task_id='join_tasks',
    dag=dag
)

# Set up dependencies between tasks
latest_currency_task >> extract_currencies_task >> load_currencies_task
latest_transaction_task >> extract_transactions_task >> load_transactions_task



# Set up dependencies between groups of tasks
[load_currencies_task, load_transactions_task] >> join_tasks