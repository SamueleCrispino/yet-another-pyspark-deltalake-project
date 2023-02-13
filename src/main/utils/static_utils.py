from pyspark.sql.types import *
from pyspark.sql.functions import *
import argparse
import re 
import pathlib

### UTILS FUNCTIONS

# computing path pasing on pruning partition conf
def get_partitions(date_range, base_path, partition_col):
    return [base_path + f"{partition_col}={date}" for date in date_range]


def set_argparse_parametes():
    parser = argparse.ArgumentParser()
    parser.add_argument(CONF_PARAMETER, help=CONF_PARAMETER_HELP, required=True)
    parser.add_argument(DATES_PARAMETER, help=DATES_PARAMETER_HELP, required=False, nargs='+')
    
    return parser

def read_query(query, env):
  sql_file = open(query, 'r')
  sql = sql_file.read()
  sql_file.close()
  REGEX = "delta.`"
  reg_result = re.search(REGEX, sql)
  # just for local tests need, in a real scenario we should be able to pass an absolute location
  # unfortunately delta-lake sql doesn't allow to use relative path within queries
  if reg_result:    
    g0 = reg_result.group(0)
    extended_path = g0 + str(pathlib.Path().resolve()) + "/test/" + env
    sql = sql.replace(g0, extended_path)
    print(sql)

  
  return sql

def get_sql_merge_condition(merge_conditions):
  sql_condition = ""
  for k, v in merge_conditions.items():
    sql_condition += f"target.{k} = updates.{v} AND "
  sql_condition += "1 = 1"
  return sql_condition

def recursive_print_json(diz):
  for k, v in diz.items():
    if not isinstance(v, dict):
      print(f"{k}: {v}")
    else:
      recursive_print_json(v)

# CLUSTER MODE
CLUSTER_MODE = "local[*]"


# ENVIRONMENT
ENV="nonprod/"

# PATHS:

### LANDING PATH
DATE_RANGE=["*"]
DATA_PATH = ENV + 'landing_zone/'

SAMPLES_PATH = DATA_PATH + 'data/'
# SAMPLE_PARTITONS = get_partitions(DATE_RANGE, SAMPLES_PATH)

DEVICES_PATH = DATA_PATH + 'devices/'

### BRONZE PATH
BRONZE_PATH = ENV + 'bronze/'
BRONZE_SAMPLES_PATH = BRONZE_PATH + 'samples/'
BRONZE_DEVICES_PATH = BRONZE_PATH + 'devices/'

### SILVER PATH
SILVER_PATH = ENV + 'silver/'
SILVER_DEVICE_SAMPLES_PATH = SILVER_PATH + 'samples/'


### GOLD PATH
GOLD_PATH = ENV + 'gold/'
GOLD_DEVICE_SAMPLES_PATH = GOLD_PATH + 'samples_devices/'

### REPORT PATH
REPORT_PATH = ENV + 'report/'
AVERAGE_REPORT_PATH = REPORT_PATH + 'averages/'


def print_conf():
    print(f"DATE_RANGE {DATE_RANGE}")
    print(f"SAMPLES_PATH {SAMPLES_PATH}")
    print(f"SAMPLE_PARTITONS {SAMPLE_PARTITONS}")
    print(f"DEVICES_PATH {DEVICES_PATH}")
    print(f"BRONZE_SAMPLES_PATH {BRONZE_SAMPLES_PATH}")
    print(f"BRONZE_DEVICES_PATH {BRONZE_DEVICES_PATH}")
    print(f"SILVER_DEVICE_SAMPLES_PATH {SILVER_DEVICE_SAMPLES_PATH}")
    print(f"GOLD_DEVICE_SAMPLES_PATH {GOLD_DEVICE_SAMPLES_PATH}")




# SCHEMAS

SAMPLE_SCHEMA = StructType([ \
    StructField("CO2_level", LongType(), True), \
    StructField("device",StringType(), False), \
    StructField("humidity", LongType(),True), \
    StructField("temperature", LongType(), True), \
    StructField("timestamp", StringType(), False), \
    StructField("received", DateType(), False) \
  ])

# COLUMN NAMES:
DEVICE_COLUMNS = ["code", "type", "area", "customer"]

# PARTITION COLUMNS
'''
having too many partitions creates too many sub-directories in a directory 
which brings unnecessarily and overhead to NameNode (if you are using Hadoop) 
since it must keep all metadata for the file system in memory.
'''
### 50.000 and 100.000 devices ==> can't be used as partition
### partitioning on "device" column could be a problem if we have too much devices
### partition pruning
BRONZE_PARTITION_COLUMNS = ["received"]  

SILVER_PARTITION_COLUMNS = ['timestamp']

GOLD_PARTITION_COLUMNS = ['month', 'area']

# COLUMNS PROPERTIES
SAMPLE_TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss.SSS'

# WINDOW FILTER
WINDOW_PARTITION = ["detailed_timestamp", "device"]
                           

# SQL
SQL = "SELECT * FROM SAMPLES WHERE timestamp > '2021-03-31' \
        AND timestamp < '2021-04-03' "

REPORT_SQL = f"""

SELECT AVG(CO2_level) as average_CO2_level, AVG(humidity) as average_humidity, AVG(temperature) as average_temperature
FROM delta.`{GOLD_DEVICE_SAMPLES_PATH}'
group by month, area

"""

### ARGPARSER PARAMS

CONF_PARAMETER = "--conf-file"
CONF_PARAMETER_HELP = "conf file location"

DATES_PARAMETER = "--dates"
DATES_PARAMETER_HELP = "ingestion days"