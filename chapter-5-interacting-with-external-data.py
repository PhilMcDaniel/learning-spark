from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#https://github.com/databricks/learningsparkv2
spark = (
    SparkSession
    .builder
    .enableHiveSupport()
    .appName("SparkSQLExampleApp")
    .getOrCreate()
)