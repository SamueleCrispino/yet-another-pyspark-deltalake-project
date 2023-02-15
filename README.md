# yet-another-pyspark-deltalake-project

This repository contains an example of pyspark delta-lake ETL configurable using appropriate json files

### Project Intro
The first version of this project was composed by a specific number of pyspark job, one for each ETL operation needed according to assignment requirements. I've left one of them job in the root/old_version_example/ folder.

After the first version my efforts have been aimed at trying to abstract the etl operations as much as possible in order to obtain a single main class that can be configured in a "light" way through json files passed as an argument.

This kind of work have increased considerably the complexity of the whole project so i apologize in advance 
if there are some logic options still not well handled.

## Project Main Components

### The JSON conf file

root/src/main/utils/jobs_etl_configs/*.json

Each jobs_etl_configs file configures a specific ETL job providing:
- spark session conf
- environment
- specific configurations for each step of the ETL Job:
1. Extract: provides source path, format, and other reading options
2. Transform (not mandatory): provides the absolute location of a SQL query containing the transform operations logic
3. Load: provides target path, format, possible definition of partitions, and other writing options

The JSON conf file must be passed as parameter "--conf-file some_etl_job.json" to the main class application's.

### The JobConfigurer Class
root/src/main/utils/JobConfigurer.py

This class contains a possible abstarction of an ETL task and is organised in order to provide:
- a useful global access for some key variables, such as:
1. a dictionary derived from the JSON conf file
2. the environment target of the application
3. a dictionary derived from possible parmaters passed to the main class 
4. all these variables are defined by the class constructor
- a SparkSession object built according to the configurations got from the JSON file
- ETL methods (Extract, Transform and Load) configurable using the information deifined in the JSON file

I've written this class mainly for the assignment' purpose but it can handle a certain set of base cases and further 
operations can be registered to improve the ETL abstraction.

### The JobRunner class
root/src/main/JobRunner

It represents the main class application's, the entry point necessary to run the whole application.
This class is a Child Class of the JobConfigurer and i've chosen this option in order to give the possibility to
override the ETL methods for any custom job.


### static_uils.py
root/src/main/utils/static_uils.py

A simple container of some useful static methods and constants.

### queries
root/src/main/utils/queries/*.sql

A set of Spark sql-dialect queries defining the logic of transform ETL's step


### build.sh
root/build.sh

A simple script for packaging python application in a zip file and possibly deploying it to a distribuited filesystem.

### Test
root/src/test

A package containing a UnitTest Class, mocks and Test Data.

Tests are runnable from src folder: python3 -m unittest test/test_etl.py 


## ETL Workflow Descritpion

The definition of each ETL was based on the following multi-hop architecture:
landing_zone --> bronze (raw data) --> silver (clean data) --> gold (enriched data) --> reports*

ETL logic has been configure in order to place all layers under a specific path representing the environment where we are supposed to work. For instance:

```
nonprod
| -- landing_zone
| -- bronze
| -- silver
| -- gold

preprod
| -- landing_zone
| -- bronze
| -- silver
| -- gold
```

*represented as CSV file, just for assignment needs

### "Bronze Device ETL"

This ETL reads Device data from landing zone and write them as delta table in the bronze layer following an UPSERT strategy.
A custom schema is enforced at reading operations, no furhter transformations are executed to the data.

### "Bronze Samples ETL"

This ETL: 
- reads samples data from landing zone
- enforces custom schema
- creates a new column "detailed_timestamp" as result of timestamp string columns parsing
- casts timestamp column as date, "detailed_timestamp" column holds the original information, this column will make easier some following operations such as the cleaning steps
- writes the result as a delta table, partitioned by received field, in the bronze layer
1. write operation follows a dynamic partitionOverwriteMode allowing to possibly "fix" landing_zone's data 

An optional argument --dates ${a list of dates yyyy-mm-dd} can be passed in order to execute a partition pruning on landing zone data

### "Silver Samples ETL"

This ETL: 
- reads samples data from bronze layer
- executes cleaning steps on data
1. deduplicates data and discards any sample which are received more than one day after they are acquired
- writes the result as a delta table in the silver layer following an UPSERT strategy
- final data are partitioned by "timestamp" column in order to optimize some specific queries

An optional argument --dates ${a list of dates yyyy-mm-dd} can be passed in order to execute a filter on the "received" partition column


### "Gold ETL"
This ETL: 
- extracts timestamp month as a new column
- performs a left join operation between samples and device data
-  writes the result as a delta table in the gold layer following an UPSERT strategy
- final data are partitioned by month and area in order to avoid shuffle during the report query execution 

### "Avg Report"
This ETL:

- represents a simple example of report by computing arithmetic mean on column containing numerical data 
- the output of the query is saved as a csv file named by date


## A few words about queries optimization...

In the context of this assignment the following reasonings have been considered:

- for the optimization of the data entry point (Bronze ETL) partition pruning operation have been performed on the "discovered partition received", in a similar way, for next ETL Job is possible to filter by "timestamp" partition columns 

- for the optimization of queries such as:

```SQL
SELECT *
FROM datalake.device_data_clean
WHERE timestamp > '2021-03-31' AND timestamp < '2021-04-05' AND
      device = '6al7RTAobR'
```
the column timestamp is used as partition for silver data. About this i have to make some considerations:
1) Basing on the fact that "in a real-world scenario, there would be between 50.000 and 100.000 devices" the device column is not a good choice to improve data access because it could lead to have too many sub-directories in a directory which brings unnecessarily and overhead to NameNode since it must keep all metadata for the file system in memory.
2) For the same reason, using a date as a partition column can be a good choice only for "small" period: an year is about 
O(10^2) partitions.
3) An alternative strategy could be to create a "month" and "year" column and perform queries on them instead of "timestamp" column.
4) As a further approach, is possible consider some connection between device code and other columns (such as type, area, customer, ..) that can be leveraged in order to achieve an optimize access to that data.

- Just in the context of this assignment i've partitioned the last layer by "month" and "area" in order to avoid shuffle operations during average computation.


## In A Real Scenario

Is it possible to submit the ETL to a Spark cluster following the command below:

```
spark-submit \
--packages io.delta:delta-core_2.12:2.2.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--py-files dependencies-1.0.0-202302130338.zip \
JobRunner.py \
--conf-file bronze_etl_samples.json
```

or adding dates parameter

```
spark-submit \
--packages io.delta:delta-core_2.12:2.2.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--py-files dependencies-1.0.0-202302130338.zip \
JobRunner.py \
--conf-file bronze_etl_samples.json \
--dates 2021-04-03 2021-04-02
```

Just be sure to specify the queries' location absolute path within JSON conf file.

As shown also by some comments, for local test purpose a '/test/' subpath is hardcoded in the stati_utils.read_query method.
This is necessary cause sql queries on delta table does not allow to use relative path for table location so in order to test it i've defined a method that builds the absolute path where test data are stored. In a real scenario it will be replaced by the absolute path of the delta-table

By the way, it's possible to submit the previous command in a local runtime environment if data are placed under a "/test/" folder just as you can see in the project structure.

In a real sceanrio zip (or egg, wheel) package, Job Runner, queries and JSON conf should be placed in a some kind of distribuited filesystem such as AWS Simple Storage where they can targeted and used by a service like AWS Glue (in a Spark Runtime environment).

The Glue Job can be parameterized in a similar way as shown above so is it possible to decouple dynamic --dates from static ETL JSON configuration.

At the end of each job, AWS glue could publish a message containing the dates processed to an SNS topic listened by an AWS Lambda Function which in turn could invoke a next glue job passing the received dates as parameter in a Serverless orchestration:

landing_zone --> {--dates} Glue Bronze ETL--> SNS {--dates} --> Lambda --> Glue Silver ETL --> ...


## Some Possible Improvements

At this stage of developments the main limits are represented by the following points:

1 - ETL Steps defined as python code in the Superclass JobConfigurer can't handle a big number of cases
- Adding new logic as python code in order to handle more complex jobs possibilities could lead to create Boilerplate code and poor maintainable classes
- It's possible to override the ETL superclass method but it would decrease configurability of the application
- So the best thing i can do in my opinion is to move the python code logic to a SQL query for the ETL steps

2 - JSON ETL conf file should be written in a more fitting format including SQL queries directly within the file in order to compact the ETL instrusction in a single location

















 
