import pyspark
import json
from datetime import date
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
from main.utils.static_utils import *



class JobConfigurer:

  # a useful global access for some key variables
  conf_dict = None
  env = None
  args = None
  
  # populating key variables
  def __init__(self, args):
    self.args = args
    with open(args["conf_file"], 'r') as config_file:
      self.conf_dict = json.load(config_file)
      self.env = self.conf_dict["env"]

  # building SparkSession object
  def get_spark_session(self):  

    spark_session_conf = self.conf_dict["spark_session"] 

    recursive_print_json(spark_session_conf)
    
    builder = pyspark.sql.SparkSession.builder\
      .master(spark_session_conf["master"])\
      .appName(spark_session_conf["app_name"])
    
    for key, val in spark_session_conf["config"].items():
      builder.config(key, val)

    return configure_spark_with_delta_pip(builder).getOrCreate()


  ### ETL METHODS

  def extract(self, spark):

    extract = self.conf_dict["extract"]
    options = extract["options"]
    options["basePath"] = set_env_path(options["basePath"], self.env)
    load_path = set_env_path(extract["load"], self.env)

    
    if "dates" in self.args and self.args["dates"]:
      dates = self.args["dates"]     
      if not "source_partition" in extract:
        raise Exception("Can't perform partition filter without explicit source_partition conf")      
      if extract['format'] != "delta": 
        load_path = get_partitions(dates, load_path, extract["source_partition"])
      else:
        return spark.read.format(extract['format'])\
          .options(**options).load(load_path)\
          .filter(col(extract["source_partition"]).isin(dates))
    
    print(options)
    print(load_path)
    print(self.args)

    return spark.read.format(extract['format']).options(**options).load(load_path)


  def transform(self, spark, df_extracted):
    if "transform" in self.conf_dict:
      transform = self.conf_dict["transform"]
      if "sql" in transform:
        sql = read_query(transform["sql"], self.env)
        df_extracted.createOrReplaceTempView("TEMP")
        df_transformed = spark.sql(sql)
      else:
        raise Exception(f"No SQL option found in transformation conf")
    else:
      return df_extracted
    return df_transformed
  
  
  def load(self, spark, df_transformed):
    load = self.conf_dict["load"]
    load_path = set_env_path(load["load_path"], self.env)

    if load["format"] == "delta":
      if load["mode"] == "merge":

        sql_merge_condition = get_sql_merge_condition(load["merge_conditions"])

        if not DeltaTable.isDeltaTable(spark, load_path):

          if "partitions" in load:
            df_transformed.write.partitionBy(load["partitions"]).format("delta").save(load_path)

          else:
            df_transformed.write.format(load["format"]).save(load_path)
        
        else:
          target = DeltaTable.forPath(spark, load_path)
          target.alias('target') \
            .merge(
              df_transformed.alias("updates"),
              sql_merge_condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
      
      elif load["mode"] == "overwrite":
        
        if "partitions" in load:
          df_transformed.write.format(load["format"])\
            .partitionBy(load["partitions"])\
            .mode(load["mode"])\
            .options(**load["options"])\
            .save(load_path)
        else:
          df_transformed.write.format(load["format"])\
            .mode(load["mode"])\
            .options(**load["options"])\
            .save(load_path)

      else:
        raise Exception(f"mode {load['mode']} not registered in JobConfigurer SuperClass")

    elif load["format"] == "csv":
      # supposed to be just for report use hence just one partition will be created
      df_transformed.repartition(1)\
            .write.format(load["format"])\
            .mode(load["mode"])\
            .options(**load["options"])\
            .save(load_path + str(date.today()))
    else:
      raise Exception(f"format {load['format']} not registered in JobConfigurer SuperClass")






