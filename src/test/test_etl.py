import unittest
import sys
sys.path.insert(1, '../main')
from main.JobRunner import JobRunner
from test.test_mock import *
import datetime
from pyspark.sql.functions import *
from delta.tables import *



class TestEtl(unittest.TestCase):


    def test_bronze_device_etl(self):
        job = JobRunner(MOCK_BRONZE_ETL_DEVICES_ARGS)
        spark = job.get_spark_session()
        df_extracted = job.extract(spark)
        
        # Testing Schema structure
        assert df_extracted.schema == DEVICE_SCHEMA

        job.load(spark, df_extracted)
    

    def test_bronze_samples_etl(self):
        job = JobRunner(MOCK_BRONZE_ETL_SAMPLES_ARGS)
        spark = job.get_spark_session()
        df_extracted = job.extract(spark)
        
        # Testing Schema structure
        assert df_extracted.schema == SAMPLE_SCHEMA

        df_transformed = job.transform(spark, df_extracted)

        # Testing new column creation
        assert "detailed_timestamp" in df_transformed.columns

        job.load(spark, df_transformed)
    

    def test_bronze_samples_etl_with_date_param(self):
        job = JobRunner(MOCK_BRONZE_ETL_SAMPLES_ARGS_DATE_PARAM)
        spark = job.get_spark_session()
        df_extracted = job.extract(spark)

        # testing date param
        date_list = [data[0] for data in df_extracted.select("received").distinct().collect()]        
        assert date_list == [datetime.date(2021, 4, 2), datetime.date(2021, 4, 1)]
    
    

    def test_silver_samples_etl(self):
        job = JobRunner(MOCK_SILVER_ETL_SAMPLES_ARGS)
        spark = job.get_spark_session()
        
        df_extracted = job.extract(spark)

        # testing duplicated data exist
        df = df_extracted\
            .groupby(["detailed_timestamp", "device"])\
            .count()\
            .filter(col("count") > 1)
        
        assert len(df.head(1)) > 0
        
        # testing samples received more than one day after they are acquired exist
        df = df_extracted.withColumn("delay", datediff(col("received"), col("timestamp")))\
            .filter(col("delay") >= 2)

        assert len(df.head(1)) > 0

        df_transformed = job.transform(spark, df_extracted)

        # testing data deduplication
        df = df_transformed\
            .groupby(["detailed_timestamp", "device"])\
            .count()\
            .filter(col("count") > 1)

        assert len(df.head(1)) == 0

        # testing samples received more than one day after they are acquired have been discarded
        df = df_transformed.withColumn("delay", datediff(col("received"), col("timestamp")))\
            .filter(col("delay") >= 2)

        assert len(df.head(1)) ==  0

        job.load(spark, df_transformed)
    
    
    def test_silver_samples_etl_with_date_param(self):
        job = JobRunner(MOCK_SILVER_ETL_SAMPLES_ARGS_DATE_PARAM)
        spark = job.get_spark_session()
        df_extracted = job.extract(spark)

        # testing date param
        date_list = [data[0] for data in df_extracted.select("received").distinct().collect()]        
        assert date_list == [datetime.date(2021, 4, 2), datetime.date(2021, 4, 1)]
        

    def test_gold_etl(self):
        job = JobRunner(MOCK_GOLD_ETL_ARGS)
        spark = job.get_spark_session()

        df_extracted = job.extract(spark)

        df_transformed = job.transform(spark, df_extracted)

        # Testing new columns
        assert "month" in df_transformed.columns
        assert "area" in df_transformed.columns        

        job.load(spark, df_transformed)



    def test_avg_report(self):
        job = JobRunner(MOCK_REPORT_ARGS)
        spark = job.get_spark_session()

        df_extracted = job.extract(spark)

        df_transformed = job.transform(spark, df_extracted)

        # Testing new columns
        assert "avg_CO2_level" in df_transformed.columns
        assert "avg_humidity" in df_transformed.columns        
        assert "avg_temperature" in df_transformed.columns      

        job.load(spark, df_transformed)



    
    if __name__ == '__main__':
        unittest.main()