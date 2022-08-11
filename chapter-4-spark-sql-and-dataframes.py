from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import image

#https://github.com/databricks/learningsparkv2

#create sparksession
spark = (
    SparkSession
    .builder
    .enableHiveSupport()
    .appName("SparkSQLExampleApp")
    .getOrCreate()
)

#data path
csv_file = 'data/departuredelays.csv'

schema = "`date` string, `delay` int, `distance` int, `origin` string, `destination` string"
df = (
    spark.read.format("csv")
    .option("inferSchema",False)
    .option("header",True)
    .load(csv_file,schema=schema)
    
)

#df.show(10,False)
#df.dtypes

#convert strings to dates
df=(
    df
    .withColumn("flightdate",to_timestamp(col("date"),"MMddhhmm"))
    .drop("date")
)
#df.show(10,False)
#create temporary view. allows us to run sql commands against it
df.createOrReplaceTempView("us_delay_flights_tbl")

#sql commands to temp view
spark.sql("""SELECT distance,origin,destination 
            FROM us_delay_flights_tbl
            WHERE distance>1000
            ORDER BY distance DESC""").show(10)

spark.sql("""SELECT flightdate, delay,origin,destination 
            FROM us_delay_flights_tbl
            WHERE delay>120 AND origin = 'SFO' AND destination = 'ORD'
            ORDER BY delay DESC""").show(10)

spark.sql("""SELECT delay,origin,destination,
                CASE
                WHEN delay > 360 THEN 'Very Long Delays'
                WHEN delay >= 120 THEN 'Long Delays'
                WHEN delay >= 60 THEN 'Short Delays'
                WHEN delay >0 THEN 'Tolerable Delays'
                WHEN delay =0 THEN 'No Delays'
                ELSE 'Early'
                END FlightDelays
            FROM us_delay_flights_tbl
            ORDER BY origin,delay DESC""").show(10)


#dataframe api instead of spark.sql yields same results
(
    df.select("distance","origin","destination")
    .where(col("distance")>1000)
    .orderBy((desc("distance")))
    .show(10)
)

#tables & views

#spark defaults to use hive metastore @ /user/hive/warehouse but you can change using spark.sql.warehouse.dir

#Managed vs Unmanaged
#managed = Spark managed metadata and data in filestore. local, HDFS, S3, blob. SQL command can drop full table (metadata & data)
#unmanaged = Spark only manages metadata, not data. External source db. Can only delete metadata

#creating databases
#via SQL
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")


#create table via SQL
spark.sql("CREATE TABLE managed_us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)")
#spark.sql("SELECT * FROM managed_us_delay_flights_tbl").show(10)

#create table via dataframe API
csv_file = 'data/departuredelays.csv'
schema = 'Date STRING, delay INT, distance INT, origin STRING, destination STRING'
flights_df = spark.read.csv(path = csv_file,schema = schema)
#flights_df.show(10)
spark.sql("DROP TABLE IF EXISTS managed_us_delay_flights_tbl")
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

#create unmanaged table via SQL
spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)
USING csv OPTIONS (PATH 'data/departuredelays.csv')"""
)

#create unmanaged table via dataframe API
(
    flights_df
    .write
    .option("path","data/departuredelays.csv")
    .saveAsTable("us_delay_flights_tbl_FROM_DATAFRAME")
)


#create views via SQL
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_vw AS
SELECT * FROM managed_us_delay_flights_tbl WHERE origin = 'SFO'
""")

spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_global_tmp_vw AS
SELECT * FROM managed_us_delay_flights_tbl WHERE origin = 'JFK'
""")

#create views via dataframe API
#create dataframe first then save dataframe as view
df_sfo=spark.sql("SELECT * FROM managed_us_delay_flights_tbl WHERE origin = 'SFO'")
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_vw")

#SELECT FROM newly created view
#global temp db is needed for global temp tables
spark.sql("""SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_vw""").show(10,False)
spark.sql("""SELECT * FROM us_origin_airport_JFK_global_tmp_vw""").show(10,False)

#drop view via SQL
spark.sql("""DROP VIEW IF EXISTS global_temp.us_origin_airport_SFO_global_tmp_vw""")
#drop view via dataframe API
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_vw")


#viewing metadata
spark.catalog.listDatabases()
spark.catalog.listTables(dbName="learn_spark_db")
spark.catalog.listColumns(tableName="managed_us_delay_flights_tbl")

#caching SQL tables
#CACHE TABLE <table_name>
#UNCACHE TABLE <table_name>

#reading tables into dataframes
df = spark.sql("SELECT * FROM learn_spark_db.managed_us_delay_flights_tbl")

#dataframe reader
file_path = 'data/2010-flight-summary.parquet'
df = spark.read.format("parquet").load(file_path)
df.show(10)

#dataframe writer
df.write.saveAsTable("learn_spark_db.2010_flight_summary_tbl")

#parquet example
#open source, default format, widely supported, columnar, compressed for i/o optimization
#stored in directory stucture (datafiles, metadata, compressed files, status files)

#read parquet into dataframe
file_path = 'data/2010-flight-summary.parquet'
df = spark.read.format("parquet").load(file_path)
df.show(10)

#write df into temp view
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_flight_delay_summary_tbl USING parquet OPTIONS(path = "data/2010-flight-summary.parquet")""")
spark.sql("""SELECT * FROM us_flight_delay_summary_tbl""").show()

#write dataframe to parquet
(
    df
    .write
    .format("parquet")
    .mode("overwrite")
    .option("compression","snappy")
    .save("data/tmp/df_parquet")
)

#write dataframe to spark sql tbl
(
    df
    .write
    .mode("overwrite")
    .saveAsTable("learn_spark_db.df_parquet")
)

#JSON

#reading JSON file into dataframe
df = spark.read.format("json").load("data/2010-summary.json")
df.show(10)

#reading JSON file into spark SQL table
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_flight_delay_2010_json_tbl USING JSON OPTIONS(path = "data/2010-summary.json")""")
spark.sql("SELECT * FROM us_flight_delay_2010_json_tbl").show(15,False)

#read table to df
df = spark.sql("SELECT * FROM us_flight_delay_2010_json_tbl")
#write df to json file
(
    df
    .write
    .format("json")
    .mode("overwrite")
    .option("compression","snappy")
    .save("data/df_json")
)


#csv

#read csv into dataframe
schema = 'DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT'
df = spark.read.format("csv").schema(schema).option("header","true").option("mode","failfast").option("nullValue","").load("data/2010-summary.csv")
df.show(10)

#read csv into spark SQL table
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_flight_delay_2010_csv_tbl USING CSV OPTIONS(path = "data/2010-summary.csv",header="true",inferSchema = "true")""")
spark.sql("SELECT * FROM us_flight_delay_2010_csv_tbl").show(10)

#write dataframe to csv file
df.write.format("csv").mode("overwrite").save("data/df_csv")


#avro
#same processes as before but errors on windows machine that I don't want to track down
df = spark.read.format("avro").load("data/2010-flight-summary.avro")


#orc
#read ORC file into dataframe
df = spark.read.format("orc").load("data/2010-summary-orc.snappy.orc")
df.show(10)

#read into spark SQL table
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_flight_delay_2010_orc_tbl USING CSV OPTIONS(path = "data/2010-summary-orc.snappy.orc")""")
spark.sql("SELECT * FROM us_flight_delay_2010_orc_tbl").show(10)

#write df to orc files
(
    df
    .write
    .format("orc")
    .mode("overwrite")
    .option("compression","snappy")
    .save("data/df_orc")
)


#images

#read image into dataframe
image_dir = 'data/images/'
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

#this blows up the terminal with data
images_df.show(10,False)
#do this instead
images_df.select("image.height","image.width","image.nChannels","image.mode").show(5,False)


#binary files
path = 'data/images/'
images_df = spark.read.format("binaryFile").option("pathGlobFilter","*.jpg").load(path)
images_df.show(5)

#cannot write back to original filetype