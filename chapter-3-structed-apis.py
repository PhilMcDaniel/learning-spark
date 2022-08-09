import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import *


spark = SparkSession.builder.appName('chapter-3-stuctured-apis').getOrCreate()
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
