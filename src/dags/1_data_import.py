from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable

import pendulum
import boto3

import vertica_python

import os
import hashlib

# Don't forget to set up variables in Airflow UI
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
conn_info =Variable.get("vertica_connection", deserialize_json=True)

def fetch_s3_file(bucket: str, key: str):
    print(key)
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename='/data/'+key
    )


def add_hash(src: str, dst: str):
    try:
        os.remove(dst)
    except OSError:
        pass
    with open(f'/data/{dst}','w',encoding='utf-8') as outfile:
        with open(f'/data/{src}','r',encoding='utf-8') as infile:
            for line in infile:
                hashed = hashlib.blake2b(line.encode('utf-8'),digest_size=16).hexdigest()
                outfile.write(str(hashed)+','+line)

def execute_sql(path: str):
    fd = open(path,'r')
    sql_string = fd.read()
    fd.close()

    with vertica_python.connect(**conn_info) as connection:
        cursor = connection.cursor()
        cursor.execute(sql_string)
        cursor.close()

def load_table_vertica(dataset: str,tablename: str):
    with vertica_python.connect(**conn_info) as connection:
        cursor = connection.cursor()
        with open(f"/data/{dataset}", 'rb') as csvfile:
            cursor.copy(f"COPY TIMALEINIKOVYANDEXRU__STAGING.{tablename} FROM STDIN DELIMITER ',' ENCLOSED BY '\"'", csvfile, buffer_size=65536)


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_project():
 
    extract_groups = PythonOperator(
        task_id='extract_groups',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'}
    )

    init_table = PythonOperator(
        task_id='init_table',
        python_callable=execute_sql,
        op_kwargs={'path': '/lessons/dags/sql/ddl_stg.sql'}
    )

    hash_groups = PythonOperator(
        task_id='hash_groups',
        python_callable=add_hash,
        op_kwargs={'src': 'group_log.csv','dst':'group_log_hashed.csv'},
    )

    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=load_table_vertica,
        op_kwargs={'dataset': 'group_log_hashed.csv','tablename':'group_log'},
    )


    fill_dds = PythonOperator(
        task_id='fill_dds',
        python_callable=execute_sql,
        op_kwargs={'path': '/lessons/dags/sql/fill_dds.sql'}
    )

    extract_groups >> hash_groups >> init_table >> load_groups >> fill_dds

dag = sprint6_project()
