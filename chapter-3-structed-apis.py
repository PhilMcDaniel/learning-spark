from math import exp
from operator import concat
import pyspark
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import avg
from pyspark.sql.types import *
from pyspark.sql.functions import *

#https://github.com/databricks/learningsparkv2

spark = SparkSession.builder.master("local[*]").appName('chapter-3-stuctured-apis').getOrCreate()
sparkContext=spark.sparkContext

#create RDD the hard way using low level apis
dataRdd = sparkContext.parallelize([("Brooke",20),("Denny",31),("Jules",30),("TD",35),("Brooke",25)])

agesRdd = (dataRdd
.map(lambda x: (x[0], (x[1],1)))
.reduceByKey(lambda x,y: (x[0]+ y[0], x[1] + y[1]))
,map(lambda x: (x[0], (x[1][0]/x[1][1])))
)

#dataframe using high level apis
data_df = spark.createDataFrame([("Brooke",20),("Denny",31),("Jules",30),("TD",35),("Brooke",25)],["name","age"])
data_df.show(10)

age_df = data_df.groupBy("name").agg((avg("age")))
age_df.show()

#can directly declare data types in scala
#so can python but there are fewer?

#benefits of explicit declaration of schema (names & types)
# - spark doesnt have to infer datatypes
# - spark doesnt have to read part of file to do the inferring
# - early detection if data doesn't match schema

#programmatic declaration of schema
schema = StructType(
    [StructField("author",StringType(),False),
    StructField("title",StringType(),False),
    StructField("pages",IntegerType(),False)]
    )
#DDL declaration of schema
schema2 = "author STRING, title STRING, pages INT"

#example
#not using single quotes but the special character
schema = "`ID` INT, `First` STRING, `Last` STRING, `URL` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
data = [
    [1, "Jules","Damnji","https://tinyurl.1","1/4/2016",4536,["twitter","LinkedIn"]],
    [2, "Brooke","Wenig","https://tinyurl.2","5/5/2018",8908,["twitter","LinkedIn"]],
    [3, "Denny","Lee","https://tinyurl.3","6/7/2019",7659,["web","fb","twitter","LinkedIn"]],
    [4, "Tathagata","Das","https://tinyurl.4","5/12/2018",10568,["twitter","fb"]],
    [5, "Matei","Zaharia","https://tinyurl.5","5/14/2014",40578,["twitter","web"]],
    [6, "Reynold","Xin","https://tinyurl.6","3/2/2015",25568,["twitter","LinkedIn"]]
]

#initialize
spark = SparkSession.builder.appName('chapter-3-campaigns').getOrCreate()
#create DF
blogs_df=spark.createDataFrame(data,schema)
#show data
blogs_df.show()
#show schema
print(blogs_df.printSchema())


#Columns & expressions
blogs_df.columns
blogs_df.dtypes
blogs_df.dtypes[0]

#use expression to compute a value
blogs_df.selectExpr(("Hits * 2")).show()
#use col to compute a value
blogs_df.select(col("Hits")*2).show()

#create new column with conditional expression
blogs_df.withColumn("Big Hitters",(expr("Hits > 10000"))).show()

#create new concatenated column
blogs_df.withColumn("AuthorsId",concat(expr("First"),expr("Last"),expr("ID"))).select("*").show()

#sort by column values
blogs_df.sort(col("ID").desc()).show()


#Rows
blog_row = Row(6,"Reynold","Xin","tinyurl.6",255568,"3/2/2015",["twitter","LinkedIn"])
blog_row[1]

#can use rows to quickly create dataframe
rows = [Row("Matei Zakaria","CA"),Row("Reynold Xin","CA")]
authors_df = spark.createDataFrame(rows,["Authors","State"])
authors_df.show()


#dataframe reader & writer
fire_schema = StructType([
    StructField('CallNumber',IntegerType(),True),
    StructField('UnitID',StringType(),True),
    StructField('IncidentNumber',IntegerType(),True),
    StructField('CallType',StringType(),True),
    StructField('CallDate',StringType(),True),
    StructField('WatchDate',StringType(),True),
    StructField('CallFinalDisposition',StringType(),True),
    StructField('AvailableDtTm',StringType(),True),
    StructField('Address',StringType(),True),
    StructField('City',StringType(),True),
    StructField('Zipcode',IntegerType(),True),
    StructField('Battalion',StringType(),True),
    StructField('StationArea',StringType(),True),
    StructField('Box',StringType(),True),
    StructField('OriginalPriority',StringType(),True),
    StructField('Priority',StringType(),True),
    StructField('FinalPriority',IntegerType(),True),
    StructField('ALSUnit',BooleanType(),True),
    StructField('CallTypeGroup',StringType(),True),
    StructField('NumAlarms',IntegerType(),True),
    StructField('UnitType',StringType(),True),
    StructField('UnitSequenceInCallDispatch',IntegerType(),True),
    StructField('FirePreventionDistrict',StringType(),True),
    StructField('SupervisorDistrict',StringType(),True),
    StructField('Neighborhood',StringType(),True),
    StructField('Location',StringType(),True),
    StructField('RowID',StringType(),True),
    StructField('Delay',FloatType(),True)
    ]
)
#DataFrameReader interfact to read csv file

sf_fire_file = "data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file,header=True,schema = fire_schema)
fire_df.show(10)

#save dataframe to .parquet file using DataFrameWriter
#winutils.exe & hadoop.dll were needed for this to work
parquet_path = 'data/sf-fire-calls.parquet'
fire_df.write.format("parquet").save(parquet_path)

#write to table
parquet_table = 'newparquettable'
fire_df.write.format("parquet").saveAsTable(parquet_table)


#dataframe transformations & actions
few_fire_df = (
    fire_df
    .select("IncidentNumber","AvailableDtTm","CallType")
    .where(col("CallType") != "Medical Incident")
)
few_fire_df.show(10,truncate= False)

#distinct counts
(fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .agg(countDistinct("CallType").alias("DistinctCallTypes"))
    .show()
)

#distinct list
(fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .distinct()
    .show()
)

#renaming, adding, dropping columns
new_fire_df = fire_df.withColumnRenamed("Delay","ResponseDelayedinMins")
(
    new_fire_df
    .select("ResponseDelayedInMins")
    .where(col("ResponseDelayedInMins")>5)
    .show(5,False)
)

#dateformatting & create/drop cols
fire_ts_df = (
new_fire_df
.withColumn("IncidentDate",to_timestamp(col("CallDate"),"MM/dd/yyyy"))
.drop("CallDate")
.withColumn("OnWatchDate",to_timestamp(col("WatchDate"),"MM/dd/yyyy"))
.drop("WatchDate")
.withColumn("AvailabilityDtTS",to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
.drop("AvailableDtTm")
.select("IncidentDate","OnWatchDate","AvailabilityDtTS")

)
fire_ts_df.show(10,False)

#use date functions now that we have date datatypes
(fire_ts_df
.select(year("IncidentDate"))
.distinct()
.orderBy(year("IncidentDate"))
.show()

)

#aggregations

(
    fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count",ascending=False)
    .show(10,False)
)

#other operations

(
    new_fire_df
    .select(sum("NumAlarms"),avg("ResponseDelayedInMins"),min("ResponseDelayedInMins"),max("ResponseDelayedInMins"))
    .show()
)

#dataset API
#datasets only make sense in Scala & Java due to bound types and Python & R using inferred
row = (350, True, "Learning Spark E2E",None)
row[0]
row[1]
row[2]

#queryplans