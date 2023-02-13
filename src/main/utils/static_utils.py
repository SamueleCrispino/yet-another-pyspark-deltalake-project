from pyspark.sql.types import *
from pyspark.sql.functions import *
import argparse
import re 
import pathlib

### UTILS FUNCTIONS

# creating env based path
def set_env_path(path, env):
  path = path.replace("{ENV}", env)
  return path

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


### ARGPARSER PARAMS

CONF_PARAMETER = "--conf-file"
CONF_PARAMETER_HELP = "conf file location"

DATES_PARAMETER = "--dates"
DATES_PARAMETER_HELP = "ingestion days"