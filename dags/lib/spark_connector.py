import datetime as dt
from string import Template

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# import os
# os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'
# os.environ['JAVA_HOME']='/usr/lib/jvm/'
# os.environ['SPARK_HOME'] ='/usr/lib/spark'


class SparkConnector():
    def __init__(self, spark_jars_packages,
                 app_name=f"app_{dt.datetime.now().strftime('%Y%m%d')}") -> None:
        self.spark = (
            SparkSession
                .builder
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.jars.packages", spark_jars_packages)
                .appName(app_name)
                .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel('ERROR')
    
    def load_df(self,
                starting_offset: int, ending_offset: int,
                kafka_options: dict) -> DataFrame:
        offsets = Template("""{"transaction-service-input":{"0":$offset,"1":$offset}}""")
        
        return (
            self.spark.read
            .format("kafka")
            .options(**kafka_options)
            .option("startingOffsets", offsets.substitute(offset=starting_offset))
            .option("endingOffsets", offsets.substitute(offset=ending_offset))
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def transform_df(self, df: DataFrame, field: str, schema,
                     **kwargs) -> DataFrame:
        df = df.withColumn('value',  F.col(field).cast('string')) \
            .withColumn('data', F.from_json(F.col('value'), schema=schema)) \
            .selectExpr('data.*')
        if kwargs:
            return df.dropDuplicates([kwargs['id']]) \
                .withWatermark(kwargs['watermark'], kwargs['interval'])
        else:
            return df