from pyspark.sql import SparkSession
import sys

app_name = sys.argv[1]
load_dt = sys.argv[2]

spark = SparkSession.builder.appName(app_name).getOrCreate()
