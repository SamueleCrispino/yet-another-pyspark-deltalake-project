import pyspark
from pyspark import SparkFiles
import argparse
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from main.utils.static_utils import *
from main.utils.JobConfigurer import JobConfigurer

from delta.tables import *

class JobRunner(JobConfigurer):

  ### overridden extract method ...

  ### overridden transform method ...

  ### overridden load  method ...


  # a compact method to run ETL operations
  def run_etl(self, spark):
    df_extracted = self.extract(spark)
    df_transformed = self.transform(spark, df_extracted)
    self.load(spark, df_transformed)
  


if __name__ == '__main__':

  # parsing job parameters
  args = vars(set_argparse_parametes().parse_args())  
  
  # initializing instance with conf 
  job = JobRunner(args)  
  
  # buildinf spark session object
  spark = job.get_spark_session()

  # running ETL task
  job.run_etl(spark)

