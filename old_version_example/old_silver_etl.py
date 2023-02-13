import pyspark
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

from utils.constants import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp").master(CLUSTER_MODE) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

print_conf()


bronze_samples = spark.read.format("delta") \
    .option("basePath", BRONZE_SAMPLES_PATH) \
    .load(BRONZE_SAMPLES_PATH) 


# window by device and timestamp
w = Window.partitionBy(WINDOW_PARTITION).orderBy(col("humidity"))

bronze_samples = bronze_samples.withColumn("record", row_number().over(w))\
              .filter(col("record") == 1).drop("record")


# filter by timestamp - received
bronze_samples = bronze_samples.withColumn("delay", datediff(col("received"), col("timestamp")))\
    .filter(col("delay") < 2).drop("delay")





print(bronze_samples.show(truncate=False))

if DeltaTable.isDeltaTable(spark, SILVER_DEVICE_SAMPLES_PATH):
  print(True)  
  
  target = DeltaTable.forPath(spark, SILVER_DEVICE_SAMPLES_PATH)
  target.alias('target') \
    .merge(
      bronze_samples.alias("updates"),
      'target.device = updates.device AND target.detailed_timestamp = updates.detailed_timestamp'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
  
else:
  print(False)
  bronze_samples.write.partitionBy(SILVER_PARTITION_COLUMNS).format("delta").save(SILVER_DEVICE_SAMPLES_PATH)


silver = spark.read.format("delta") \
    .option("basePath", SILVER_DEVICE_SAMPLES_PATH) \
    .load(SILVER_DEVICE_SAMPLES_PATH) 


print(silver.groupBy("received").count().show(truncate=False))
print(silver.printSchema())