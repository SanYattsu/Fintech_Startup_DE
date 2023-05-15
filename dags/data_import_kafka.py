from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType
)
from pyspark.sql import functions as F

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable

import pendulum
import datetime as dt
import logging
import vertica_python
import clickhouse_driver

from lib.spark_connector import SparkConnector


vertica_conn = BaseHook.get_connection('vertica_conn')
vertica_extras = BaseHook.get_connection('vertica_conn').extra_dejson
vertica_conn_info = dict(
    host=vertica_conn.host,
    port=vertica_conn.port,
    user=vertica_conn.login,
    password=vertica_conn.password,
    database=vertica_extras['database'],
    connection_timeout=30
)

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


kafka_options = {
    'subscribe': 'transaction-service-input',
    'kafka.bootstrap.servers': 'rc1a-083hhcof7uqe71hd.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"producer_consumer\" password=\"{Variable.get("KAFKA_PASSWORD")}";',
    'kafka.ssl.truststore.location': '/ca/YandexInternalRootCA.jks',
    'kafka.ssl.truststore.password': Variable.get("CA_PASSWORD")
}


spark_jars_packages = ",".join(
    [
        "org.postgresql:postgresql:42.4.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    ]
)


msg_schema = StructType([
    StructField("object_id", StringType(), nullable=True),
    StructField("object_type", StringType(), nullable=True),
    StructField("sent_dttm", TimestampType(), nullable=True),
    StructField("payload", StringType(), nullable=True),
])

transaction_schema = StructType([
    StructField("operation_id", StringType(), nullable=True),
    StructField("account_number_from", IntegerType(), nullable=True),
    StructField("account_number_to", IntegerType(), nullable=True),
    StructField("currency_code", IntegerType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("transaction_type", StringType(), nullable=True),
    StructField("amount", IntegerType(), nullable=True),
    StructField("transaction_dt", TimestampType(), nullable=True),
])


@dag(
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=False,
    description='Загрузка данных из kafka топика в vertica и clickhouse',
    tags=['stg', 'transactions', 'vertica', 'kafka', 'clickhouse'],
    max_active_runs = 1,
    is_paused_upon_creation=True,
    default_args={
        'owner': 'airflow',
        'email_on_failure': False,
        'retries': 2,
        'retry_delay': dt.timedelta(minutes=5),
    },
)
def kafka2olap_dag():
    @task
    def olap_load():
        spark = SparkConnector(spark_jars_packages)

        # Читаем все сообщения
        df = spark.load_df(0, -1, kafka_options)

        # Раскрываем данные из топика
        kwargs = {
            'id': 'object_id',
            'watermark': 'sent_dttm',
            'interval': '60 minutes'
        }
        tr_df = spark.transform_df(df, 'value', msg_schema, **kwargs)

        # Получаем датафрейм транзакций
        tr_df = tr_df.filter(F.col('object_type') == F.lit('TRANSACTION'))
        transactions = spark.transform_df(tr_df, 'payload', transaction_schema)

        try:
            # Формирум данные вида [{k:v}, {k:v}] перед отправкой в vertica
            data = [obj.asDict() for obj in transactions.toLocalIterator()]
            assert len(data) == 0
        except AssertionError:
            logging.error('Данные отсутствуют!')
            raise

        columns = list(data[0].keys())

        # Загружаем данные в vertica
        try:
            with vertica_python.connect(**vertica_conn_info) as conn:
                with conn.cursor() as cur:
                    cur.executemany(f"""INSERT INTO SANYATTSUYANDEXRU__STAGING.transactions ({','.join(columns)})
                                    VALUES ({''.join([':' + x + ',' for x in columns])[:-1]});""",
                                    data, use_prepared_statements=False)
        except Exception as e:
            logging.error(f"Error loading data into Vertica: {e}")
            raise

        # Загружаем данные в clickhouse
        try:
            with clickhouse_driver.connect(**clickhouse_conn_info) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        f"INSERT INTO dwh.transactions ({','.join(columns)}) VALUES",
                        [list(x.values()) for x in data]
                    )
        except Exception as e:
            logging.error(f"Error loading data into ClickHouse: {e}")
            raise


    olap_load()


_ = kafka2olap_dag()
