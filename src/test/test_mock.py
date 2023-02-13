from pyspark.sql.types import *
from pyspark.sql.functions import *

MOCK_BRONZE_ETL_DEVICES_ARGS = {
    'conf_file': 'main/utils/jobs_etl_configs/bronze_etl_devices.json', 
    'dates': None
}

MOCK_BRONZE_ETL_SAMPLES_ARGS = {
    'conf_file': 'main/utils/jobs_etl_configs/bronze_etl_samples.json', 
    'dates': None
}

MOCK_BRONZE_ETL_SAMPLES_ARGS_DATE_PARAM = {
    'conf_file': 'main/utils/jobs_etl_configs/bronze_etl_samples.json', 
    'dates': ["2021-04-02", "2021-04-01"]
}

MOCK_SILVER_ETL_SAMPLES_ARGS = {
    'conf_file': 'main/utils/jobs_etl_configs/silver_etl.json', 
    'dates': None
}

MOCK_SILVER_ETL_SAMPLES_ARGS_DATE_PARAM = {
    'conf_file': 'main/utils/jobs_etl_configs/silver_etl.json', 
    'dates':  ["2021-04-02", "2021-04-01"]
}

MOCK_GOLD_ETL_ARGS = {
    'conf_file': 'main/utils/jobs_etl_configs/gold_etl.json', 
    'dates': None
}

MOCK_REPORT_ARGS = {
    'conf_file': 'main/utils/jobs_etl_configs/avg_report.json', 
    'dates': None
}




DEVICE_SCHEMA = StructType([ \
    StructField("code", StringType(), True), \
    StructField("type", StringType(), True), \
    StructField("area", StringType(), True), \
    StructField("customer", StringType(), True) \
  ])

SAMPLE_SCHEMA = StructType([ \
    StructField("CO2_level", LongType(), True), \
    StructField("device",StringType(), True), \
    StructField("humidity", LongType(),True), \
    StructField("temperature", LongType(), True), \
    StructField("timestamp", StringType(), True), \
    StructField("received", DateType(), True) \
  ])