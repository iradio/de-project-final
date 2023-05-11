from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}