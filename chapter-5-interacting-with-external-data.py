from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
import pandas as pd

#https://github.com/databricks/learningsparkv2
spark = (
    SparkSession
    .builder
    .enableHiveSupport()
    .appName("SparkSQLExampleApp")
    .getOrCreate()
)

#spark SQL UDFs
def cubed(s):
    return s*s*s

#register UDF
spark.udf.register("cubed",cubed,LongType())

spark.range(1,9).createOrReplaceTempView("udf_test")
spark.sql("SELECT id,cubed(id) FROM udf_test").show()


#evaluation order of WHERE clause is not guaranteed. Has implications for NULL checking

#vectorized/Pandas UDF to improve performance (not row by row execution)
#declare the function
def cubed_pd(a:pd.Series) -> pd.Series:
    return a*a*a

#create the udf for the function
cubed_pd_udf = pandas_udf(cubed_pd,returnType=LongType())

#create series of test data
x = pd.Series([1,2,3,4,5,6,7,8,9])
#execute from pandas
print(cubed_pd(x))

#execute the spark udf
df=spark.range(1,9)
df.select("id",cubed_pd_udf(col("id"))).show()
#checking the spark UI shows a WholeStageCodegen DAG step which indicates better performance
#ArrowEvalPython DAG step implies pandas UDF usage/execution