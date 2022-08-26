from statistics import mode
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import *
from pyspark.storagelevel import *
from delta import *

#https://github.com/databricks/learningsparkv2
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") #needed for delta
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") #needed for delta
    .enableHiveSupport()
    .appName("chapter-9")
    .getOrCreate()
)


#desired properties of storage

#scalable & performance
#transaction support (ACID, reads & writes)
#diverse data formats (unstructured, semi-structured)
#diverse workloads (BI, ETL, streaming, ML/AI)
#openness (standard API, multiple tools/engines)

#databases -> data lakes -> data lakehouses

#databases
#spark mainly for OLAP, not OLTP (analytics not transactional)
#databases limited by data size growth
#not great with new use cases (AI/ML)
#expense to scale
#do not support non-SQL analytics

#data lakes
#supports diverse workloads (batch, ETL, SQL, streaming,ML)
#supports diverse file formats (stuctured, semi, un)
#supports diverse file systems (hadoop, APIs)
#limitations
#atomicity & isolation (no rollback or transaction isolation)
#consistency (inconsistent schema/file format, bad data quality)
#workarounds
#partitioned folders
#full rewrites to ensure ACID/atomic deletes/updates
#complex update schedules to avoid concurrent access inconsistencies

#data lakehouse
#features
#ACID transaction support
#schema enforcement & governance
#diverse data types in open formats
#diverse workloads
#upserts & deletes
#data governance (integrity & audit)
#open storage that has (transaction log, table versioning for snapshot isolation, reading/writing w/ spark)

#delta lake supports
#streaming
#update, delete, merge
#schema evolution
#time travel
#rollback
#serial isolation


#configuring DeltaLake
#pyspark --packages io.delta:delta-core_2.12:0.7.0

source_path = 'data/loan-risks.snappy.parquet'
delta_path = 'data/tmp/loans_delta'
(
    spark.read.format("parquet").load(source_path).write.format("delta").save(delta_path)
)