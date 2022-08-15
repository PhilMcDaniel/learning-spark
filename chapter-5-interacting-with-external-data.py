from multiprocessing.dummy import Array
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
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



#higher order functions in Dataframes & Spark SQL

#EXPLODE() to turn nested data into tabular, row format

#numerous array() built-in functions for working with lists

schema = StructType([StructField("celcius",ArrayType(IntegerType()))])
t_list = [[35,36,32,30,40,42,38]],[[31,32,34,55,56]]
t_c = spark.createDataFrame(t_list,schema)
t_c.createOrReplaceTempView("tC")

spark.sql("SELECT * FROM tC").show()

#using transform()
spark.sql("""SELECT celcius,transform(celcius,t -> ((t*5) div 5) + 32) as fahrenheit from tC""").show()

#using filter()
#creates array of values where boolean is true
spark.sql("""SELECT celcius, filter(celcius, t-> t>38) as high from tC""").show()


#using exists()
#returns true if any value of boolean is true
spark.sql("""SELECT celcius, exists(celcius, t-> t=38) threshold from tC""").show()


#reduce()
#flattens array to single value by applying function
#not working for some reason. Functiond doesn't exist?
spark.sql("""SELECT celcius, reduce(celcius,0,(t,acc)-> t+acc,acc -> (acc div size(celcius)*9 div 5)+32)avgFahrenheit from tC""").show()



#common dataframe & sql operations

trip_delays = 'data/departuredelays.csv'
airports = 'data/airport-codes-na.txt'

airports_na = (spark.read.format("csv").options(header="true",inferSchema = "true",sep="\t").load(airports))
#airports_na.show(10,False)

delays = (spark.read.format("csv").options(header="true").load(trip_delays))
#delays.show(10,False)

#add new cols
delays.withColumn("delay",expr("CAST(delay as int) as delay"))
delays.withColumn("distance",expr("CAST(distance as int) as distance"))

delays.createOrReplaceTempView("departureDelays")

#create smaller filtered table
foo = (delays.filter(expr("""origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")
#foo.show(10,False)


#unions
bar=delays.union(foo)
bar.createOrReplaceTempView("bar")

bar.filter("""origin =='SEA' AND destination = 'SFO' AND date like '01010%' and delay>0""").show()
spark.sql("""SELECT * FROM bar where origin = 'SEA' and destination = 'SFO' AND date LIKE '01010%' and delay > 0""").show()

#joins
#all normal types (outer, inner, full, cross, semi, anti)

#window functions
#normal options, need to partition data (bound window size) because it would be done on a single executor (can cause perf issues as show in my chess project)
spark.sql(" DROP TABLE IF EXISTS departurewindow;")
spark.sql(
" CREATE TABLE departurewindow AS "+
" SELECT origin,destination,sum(delay) totaldelays"+
" from departureDelays"+
" WHERE origin in ('SEA','SFO','JFK','DEN','ORD','LAX','ATL')"+
" GROUP BY origin, destination")

spark.sql("""SELECT * FROM departurewindow""").show()

spark.sql("""
SELECT origin, destination,TotalDelays,rank
 FROM (
       SELECT origin, destination, totaldelays, dense_rank() OVER(PARTITION BY origin ORDER BY TotalDelays DESC) as rank
      FROM departurewindow)t
 WHERE rank <=3
""").show()

