from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.decorators import dag
from airflow.models import Variable

from datetime import datetime, timedelta

from default_args import default_args

import vertica_python

# import the logging module
import logging
logger = logging.getLogger("airflow.task")

# Don't forget to set up Connection and Variable in Airflow UI
vertica_connection = Variable.get("vertica_connection", deserialize_json=True)
stg_schema_name = Variable.get("stg_schema_name", default_var='TIM_ALEINIKOV_YANDEX_RU__STAGING')
cdm_schema_name = Variable.get("cdm_schema_name", default_var='TIM_ALEINIKOV_YANDEX_RU__DWH')
cdm_table_name = Variable.get("cdm_table_name", default_var='global_metrics')

def calculate_global_metrics(connection, table_name, **context):
    # достаем дату запуска DAG и сдвигаем на -1 день для извлечения данных за "вчера"
    execution_date = (context['logical_date'] - timedelta(days=1) ).strftime('%Y-%m-%d') 

    # SQL string definition
    delete_sql = f"""
        delete from {cdm_schema_name}.{table_name}
        where date_update = to_date('{execution_date}','yyyy-mm-dd')    
    """

    insert_sql = f"""
        insert into {cdm_schema_name}.{table_name}
        with chargeback as (
            select operation_id 
            from {stg_schema_name}.transactions t
            where status = 'chargeback'
        ),
        r as (
            select 
            to_char(t.transaction_dt,'yyyy-mm-dd') as transaction_day
            , t.operation_id
            ,t.currency_code as  currency_from
            , t.amount as amount 
            , COALESCE(c.currency_with_div, 1) as usd_currency_div
            , account_number_from as account_make_transaction
            from {stg_schema_name}.transactions t
            left join chargeback cb using (operation_id)
            left join {stg_schema_name}.currencies c 
                on to_char(c.date_update,'yyyy-mm-dd') = to_char(t.transaction_dt,'yyyy-mm-dd')
                                and c.currency_code_with = 420 -- выбираем курс к доллару
                                and t.currency_code  = c.currency_code
            where cb.operation_id is null
        		and transaction_day = '{execution_date}' -- выбираем только один день
                and account_number_from > 0 -- убираем тестовые аккаунты
                and account_number_to > 0 -- убираем тестовые аккаунты
                and t.status = 'done' -- оставляем только завершенные транзакции
                and t.transaction_type in ('c2a_incoming',
                                            'c2b_partner_incoming',
                                            'sbp_incoming',
                                            'sbp_outgoing',
                                            'transfer_incoming',
                                            'transfer_outgoing')
            order by t.operation_id
        ) 
        select 
            to_date(r.transaction_day,'yyyy-mm-dd') as date_update
            , r.currency_from::int as currency_from
            , sum(r.amount * r.usd_currency_div)::numeric(18,2) as amount_total 
            , count(distinct operation_id)::int as cnt_transactions
            , count(distinct account_make_transaction)::int as cnt_accounts_make_transactions 
            , cnt_transactions / cnt_accounts_make_transactions::numeric(16,2) as avg_transactions_per_account
        from r
        group by r.transaction_day, r.currency_from
        order by r.transaction_day, r.currency_from
    """

    conn = vertica_python.connect(**connection)
    cursor = conn.cursor()

    logger.info(f"Delete values from {cdm_schema_name}.{table_name} by date {execution_date}")
    cursor.execute(delete_sql)

    logger.info(f"Insert values into: {cdm_schema_name}.{table_name} fro {execution_date}")
    cursor.execute(insert_sql)

    conn.commit()
    
    cursor.close()
    conn.close()

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2020, 12, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=30)
# }

dag = DAG('cdm_global_metrics', default_args=default_args, schedule_interval='@daily')

calculate_global_metrics_task = PythonOperator(
    task_id='calculate_global_metrics_task',
    python_callable=calculate_global_metrics,
    op_kwargs={"connection": vertica_connection,
               "table_name": cdm_table_name
               },
    provide_context=True,
    dag=dag
)

# Set up task order
calculate_global_metrics_task