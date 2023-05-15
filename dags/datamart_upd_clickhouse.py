from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

import pendulum
import datetime as dt
import logging
import clickhouse_driver

clickhouse_conn = BaseHook.get_connection('clickhouse_conn')
clickhouse_extras = BaseHook.get_connection('clickhouse_conn').extra_dejson
clickhouse_conn_info = dict(
    host=clickhouse_conn.host,
    port=clickhouse_conn.port,
    user=clickhouse_conn.login,
    password=clickhouse_conn.password,
    database=clickhouse_extras['database'],
    secure=True,
    ca_certs='/ca/YandexInternalRootCA.crt'
)


@dag(
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2022, 10, 2, tz="UTC"),
    end_date=pendulum.datetime(2022, 11, 1, tz="UTC"),
    catchup=True,
    description='Актуализация global_metrics для визуализации в DataLens',
    tags=['datamart', 'transactions', 'clickhouse', 'DataLens'],
    max_active_runs = 4,
    is_paused_upon_creation=True,
    default_args={
        'owner': 'airflow',
        'email_on_failure': False,
        'retries': 2,
        'retry_delay': dt.timedelta(minutes=5),
    },
)
def upd_datamart():
    @task
    def insert_global_metrics(business_dt: str):
        with open("./sql/clickhouse_datamart.sql", "rb") as f:
            query = (''.join([x.decode("utf-8") for x in f.readlines()]))

        # Загружаем данные в clickhouse
        logging.info(f"Starting insert_global_metrics for business_dt={business_dt}")
        try:
            with clickhouse_driver.connect(**clickhouse_conn_info) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, {'business_dt': business_dt})
        except Exception as e:
            logging.error(f"Error loading data into ClickHouse: {e}")
            raise

        logging.info(f"Starting insert_global_metrics for business_dt={business_dt}")

    insert_global_metrics('{{ ds }}')


_ = upd_datamart()