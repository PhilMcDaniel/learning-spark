import sys

from pyspark.sql import SparkSession

#read file and aggregate results
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>",file=sys.stderr)
        sys.exit

#build sparksession
spark = (SparkSession
.builder
.appName("PythonMnMCount")
.getOrCreate())

#get MnM dataset filename from cmd line args
mnm_file = sys.argv[1]

#readfile into Spark dataframe using the csv format & other options
mnm_df = (spark.read.format("csv")
.option("header", "true")
.option("inferSchema","true")
.load(mnm_file)
)

#transforms for all states
count_mnm_df = (mnm_df
.select("State","Color","Count")
.groupBy("State","Color")
.sum("Count")
.orderBy("sum(Count)",ascending = False)
)

#action
count_mnm_df.show(n=60,truncate = False)
print("Total rows = %d" % (count_mnm_df.count()))


#single state filtering transform


#single state action 

#figure out how the localhost url monitor works

spark.stop()