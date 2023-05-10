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
# get the airflow.task logger
logger = logging.getLogger("airflow.task")


# Don't forget to set up Connection and Variable in Airflow UI
# postgres_connection = 'postgres_connection'
postgres_connection = Variable.get("postgres_connection", deserialize_json=True)
vertica_connection = Variable.get("vertica_connection", deserialize_json=True)
schema_name = 'TIM_ALEINIKOV_YANDEX_RU__STAGING'
batch_size = 1000


def extract_table(connection, table_name, orderby, **context):
    # достаем дату запуска DAG и сдвигаем на -1 день для извлечения данных за "вчера"
    execution_date = (context['logical_date'] - timedelta(days=1) ).strftime('%Y-%m-%d') 
    logger.info(f"Execution date is {execution_date}")

    # conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    conn = psycopg2.connect(**connection)
    cur = conn.cursor()
    
    offset = 0
    rest = 1
    files = []
    while rest != 0 :

        # Execute the query to select all rows from the table
        sql = f"SELECT * FROM public.{table_name} WHERE to_char({orderby},'yyyy-mm-dd') ='{execution_date}' LIMIT {batch_size} OFFSET {offset}"
        logger.info(f"SQL satement: {sql}")
        cur.execute(sql)

        # Fetch all rows from the query result
        rows = cur.fetchall()
        rest = len(rows)
        logger.info(f"rows: {rest}")
        if rest == 0:
            break
        
        filepath = f"/tmp/airflow/stg_{table_name}_{execution_date}_{offset // batch_size}.csv"
        
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
    # logger.info(f"Files: {files}")
    context['ti'].xcom_push(key=f'files_{table_name}_{execution_date}', value=json.dumps(files))

def read_csv_file(csv_file_path):
    with open(csv_file_path) as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def load_table(connection, table_name, orderby, **context):
    # достаем дату запуска DAG и сдвигаем на -1 день для извлечения данных за "вчера"
    execution_date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d') 

    execution_date_under = str(execution_date).replace('-','_')
    files = json.loads(context['ti'].xcom_pull(key=f'files_{table_name}_{execution_date}'))
    logger.info(f"Files: {files}")

    # SQL string definition
    copy_sql = f"""
        COPY {schema_name}.{table_name}_{execution_date_under} 
        FROM STDIN DELIMITER ',' ENCLOSED BY '\"' NULL AS ''
        DIRECT STREAM NAME 'stg_stream'
        REJECTED DATA AS TABLE {schema_name}.rejected_data;
    """
    drop_temp_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name}_{execution_date_under};"
    
    if (table_name == 'transactions'):
        create_ddl = f"""
        DROP TABLE IF EXISTS {schema_name}.{table_name}_{execution_date_under};

        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}_{execution_date_under} (
            operation_id varchar(60) NULL,
            account_number_from int NULL,
            account_number_to int NULL,
            currency_code int NULL,
            country varchar(30) NULL,
            status varchar(30) NULL,
            transaction_type varchar(30) NULL,
            amount int NULL,
            transaction_dt TIMESTAMP(0) NULL,
            load_id IDENTITY
        )
        order by transaction_dt
        SEGMENTED BY hash(operation_id,transaction_dt) all nodes
        PARTITION BY COALESCE(transaction_dt::date,'1900-01-01');
        """
        drop_duplicates_sql = f"""
        DELETE FROM {schema_name}.{table_name}_{execution_date_under}
            WHERE load_id in (
                SELECT load_id from (
                    SELECT load_id , ROW_NUMBER() OVER(
                        partition by operation_id
                            ,account_number_from
                            ,account_number_to
                            ,currency_code
                            ,country
                            ,status
                            ,transaction_type
                            ,amount
                            ,transaction_dt
                        order by load_id) as rnum
                    FROM {schema_name}.{table_name}_{execution_date_under}
                ) s 
                WHERE rnum > 1
            );
        """
        merge_sql = f"""
        MERGE INTO {schema_name}.{table_name} t
        USING {schema_name}.{table_name}_{execution_date_under} s
        ON t.operation_id = s.operation_id
            and t.account_number_from = s.account_number_from
            and t.account_number_to = s.account_number_to
            and t.transaction_dt = s.transaction_dt
        WHEN MATCHED THEN UPDATE SET 
            currency_code = s.currency_code,
            country = s.country,
            status = s.status,
            transaction_type = s.transaction_type,
            amount = s.amount
        WHEN NOT MATCHED THEN INSERT (
            account_number_from
            ,account_number_to
            ,currency_code
            ,country
            ,status
            ,transaction_type
            ,amount
            ,transaction_dt
            ,operation_id) 
        VALUES (
            s.account_number_from
            ,s.account_number_to
            ,s.currency_code
            ,s.country
            ,s.status
            ,s.transaction_type
            ,s.amount
            ,s.transaction_dt
            ,s.operation_id);
        """
    elif (table_name == 'currencies'):
        create_ddl = f"""
        DROP TABLE IF EXISTS {schema_name}.{table_name}_{execution_date_under};

        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}_{execution_date_under} (
            date_update TIMESTAMP(0) NULL,
            currency_code int NULL,
            currency_code_with int NULL,
            currency_with_div NUMERIC(5, 3) NULL,
            load_id IDENTITY
        );
        """
        drop_duplicates_sql = f"""
        DELETE FROM {schema_name}.{table_name}_{execution_date_under}
            WHERE load_id in (select load_id from (
                SELECT load_id , ROW_NUMBER() OVER(
                    partition by date_update, currency_code, currency_code_with, currency_with_div
                    order by load_id) as rnum
                FROM {schema_name}.{table_name}_{execution_date_under}
            ) s 
            WHERE rnum > 1)
        """
        merge_sql = f"""
        MERGE INTO {schema_name}.{table_name} t
        USING {schema_name}.{table_name}_{execution_date_under} s
        ON (t.currency_code = s.currency_code) and (t.currency_code_with = s.currency_code_with) and (t.date_update = s.date_update)
        WHEN MATCHED THEN UPDATE SET currency_with_div = s.currency_with_div
        WHEN NOT MATCHED THEN INSERT (date_update, currency_code, currency_code_with, currency_with_div) 
        VALUES (s.date_update, s.currency_code, s.currency_code_with, s.currency_with_div);
        """
    else:
        logger.warning(f"unknown table {table_name}. Skipping...")
        return


    conn = vertica_python.connect(**connection)
    cursor = conn.cursor()

    
    logger.info(f"Creating temporary table {table_name}")
    cursor.execute(create_ddl)

    for filepath in files:
        with open(filepath, 'rb') as csvfile:
            logger.info(f"Load file {filepath} to table: {schema_name}.{table_name}_{execution_date_under}")
            cursor.copy(copy_sql, csvfile, buffer_size=65536)
            
        conn.commit()
        os.remove(filepath)
        logger.info(f'Remove temp file: {filepath}')
    
    logger.info(f"Drop duplicates in table: {schema_name}.{table_name}_{execution_date_under}")
    cursor.execute(drop_duplicates_sql)
    logger.info(f"Execute merge SQL: {schema_name}.{table_name}_{execution_date_under} INTO {schema_name}.{table_name}")
    cursor.execute(merge_sql)
    logger.info(f"Drop temp table: {schema_name}.{table_name}_{execution_date_under}")
    cursor.execute(drop_temp_sql)

    conn.commit()
    
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

dag = DAG('stg_currencies_and_transactions', default_args=default_args, schedule_interval='@daily')

extract_currencies_task = PythonOperator(
    task_id='extract_currencies',
    python_callable=extract_table,
    op_kwargs={"connection": postgres_connection,
               "table_name":"currencies",
               "orderby": "date_update"
               },
    provide_context=True,
    dag=dag
)

load_currencies_task = PythonOperator(
    task_id='load_currencies',
    python_callable=load_table,
    op_kwargs={"connection": vertica_connection,
               "table_name":"currencies",
               "orderby": "date_update"
               },
    provide_context=True,
    dag=dag
)

extract_transactions_task = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_table,
    op_kwargs={"connection": postgres_connection,
               "table_name":"transactions",
               "orderby": "transaction_dt"
               },
    provide_context=True,
    dag=dag
)

load_transactions_task = PythonOperator(
    task_id='load_transactions',
    python_callable=load_table,
    op_kwargs={"connection": vertica_connection,
               "table_name":"transactions",
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
extract_currencies_task >> load_currencies_task
extract_transactions_task >> load_transactions_task

# Set up dependencies between groups of tasks
[load_currencies_task, load_transactions_task] >> join_tasks