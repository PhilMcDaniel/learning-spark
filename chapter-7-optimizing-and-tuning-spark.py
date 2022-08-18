from statistics import mode
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import *
from pyspark.storagelevel import *


#https://github.com/databricks/learningsparkv2


#configs are int he $SPARK_HOME ./home/ dir (.template files)
#change values & remove the .template suffix will tell Spark to use the values from the file

#can also set values when using spark-submit command using the "--conf" flag (--conf spark.sql.shuffle.partitions=5)
#can also set when building session

spark = (
    SparkSession
    .builder
    .config("spark.sql.shuffle.partitions",5)
    .config("spark.shuffle.service.enabled","true")
    .config("spark.dynamicAllocation.enabled","true")
    .config("spark.executor.memory","2g")
    .config("spark.driver.memory","1g")
    .master("local[*]")
    .enableHiveSupport()
    .appName("Chapter-7")
    .getOrCreate()
)

#all configs in pyspark
spark.sparkContext.getConf().getAll()

#all configs 2nd way using sql
#this seems nice to me, but -SET isn't straightforward so I'll probably forget
spark.sql("SET -v").select("key","value").show(50,False)

#can also view configs in the Environment tab of Spark UI


#get/set configs
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.shuffle.partitions",6)


#scaling spark for large workloads
#driver, executor, shuffle service

#static vs dynamic resource allocation
#when sending specific parms via spark-submit, you can't go over those values
#dynamic especially useful for streaming & on-demand analytics where volume fluctuates greatly

#below setting changes require the following two values be set first
spark.conf.get("spark.shuffle.service.enabled")#set in app build
spark.conf.get("spark.dynamicAllocation.enabled")#set in app build

#trouble setting these locally like this. Maybe all need to be set in app build or I'm missing a dependency
spark.conf.get("spark.dynamicAllocation.minExecutors")
spark.conf.get("spark.dynamicAllocation.schedulerBacklogTimeout")
spark.conf.get("spark.dynamicAllocation.maxExecutors")
spark.conf.get("spark.dynamicAllocation.executorIdleTimeout")


#common config updates
spark.conf.set("spark.driver.memory","1g")#change via spark-submit or on app creation
spark.conf.set("spark.shuffle.file.buffer","1m") #default is 32kb recommended is 1mb
spark.conf.set("spark.io.compression.lz4.blockSize","512k") #default is 32kb recommended is 512kb


#maximizing parallelism
spark.conf.get("spark.sql.files.maxPartitionBytes")
spark.conf.get("spark.sql.shuffle.partitions")


#caching and persistance
#df.cache() will store as many partitions as possible in memory
df = spark.range(1 * 10000000).toDF("id").withColumn("square",col("id") * col("id"))
df.show(10,False)
df.cache()
df.count()
#count is much quicker the second time after the cache is materialized

#df.persist() provides control on how data is cached
df = spark.range(1 * 10000000).toDF("id").withColumn("square",col("id") * col("id"))
df.persist(StorageLevel.DISK_ONLY)
#materialize the cache
df.count()
#retrieve from cache(much quicker)
df.count()

#have to unpersist when stored on disk
df.unpersist() #validate in SparkUI to see the storage and see it disappear

#can also cache tables/views
df.createOrReplaceTempView("dfTable")
spark.sql("CACHE TABLE dfTable")
spark.sql("SELECT COUNT(*) from dfTable").show()

#cache when df is used repeatedly for queries or transformations
#do not cache when DF is too large to fit in memory or infrequent use


#family of spark joins
df.explain(mode="simple") #use this to see the join operation
#broadcast hash join


#shuffle sort merge join