from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from airflow.sensors.s3_key_sensor import S3KeySensor

import io
import pendulum
import datetime as dt
import logging
import vertica_python
import clickhouse_driver
import pandas as pd

from lib.s3_connector import S3Connector

BUCKET_KEY = 'currencies_history.csv'
BUCKET_NAME = 'final-project'
s3_conn = S3Connector(Variable.get("AWS_ACCESS_KEY_ID"),
                      Variable.get("AWS_SECRET_ACCESS_KEY"))


vertica_conn = BaseHook.get_connection('vertica_conn')
vertica_extras = BaseHook.get_connection('vertica_conn').extra_dejson
vertica_conn_info = {
    'host': vertica_conn.host,
    'port': vertica_conn.port,
    'user': vertica_conn.login,
    'password': vertica_conn.password,
    'database': vertica_extras['database'],
    'connection_timeout': 30
}

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
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=False,
    description='Загрузка данных из s3 хранилища',
    tags=['stg', 'currency', 's3', 'clickhouse'],
    max_active_runs = 1,
    is_paused_upon_creation=True,
    default_args={
        'owner': 'airflow',
        'email_on_failure': False,
        'retries': 2,
        'retry_delay': dt.timedelta(minutes=5),
    },
)
def s3_download_dag():
    s3_file_check = S3KeySensor(
        task_id='s3_file_check',
        bucket_key=BUCKET_KEY,
        bucket_name=BUCKET_NAME,
        aws_conn_id='yandexcloud-s3',
        wildcard_match=True,
        poke_interval=60,
        timeout=360,
    )

    @task
    def s3_fetch_and_load(bucket: str, key: str):
        s3_bucket = s3_conn.s3_resourse.Bucket(bucket)

        # Пишем данные в буфер
        buffer = io.BytesIO()
        s3_bucket.download_fileobj(key, buffer)
        buffer.seek(0)

        # Первая строка в csv файле - заголовки
        columns = buffer.readline().decode("utf-8").rstrip()
        # logging.debug(columns)

        # Загружаем данные в vertica
        buffer.seek(0)
        try:
            with vertica_python.connect(**vertica_conn_info) as conn:
                with conn.cursor() as cur:
                    cur.copy(f"""
                        COPY SANYATTSUYANDEXRU__STAGING.currencies ({columns})
                        FROM stdin
                        DELIMITER ','
                    """, buffer.getvalue())
        except Exception as e:
            logging.error(f"Error loading data into Vertica: {e}")
            raise


        # Загружаем данные в clickhouse
        buffer.seek(0)
        try:
            with clickhouse_driver.connect(**clickhouse_conn_info) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        f"INSERT INTO dwh.currencies ({columns}) VALUES",
                        pd.read_csv(buffer, parse_dates=[2])
                            .to_dict('split')['data']
                    )
        except Exception as e:
            logging.error(f"Error loading data into ClickHouse: {e}")
            raise


    s3_file_check
    s3_fetch_and_load(BUCKET_NAME, BUCKET_KEY)


_ = s3_download_dag()
