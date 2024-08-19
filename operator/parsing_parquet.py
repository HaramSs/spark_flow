from pyspark.sql import SparkSession
import sys
import os

# LOAD_DT = sys.argv[1]

# logFile = "/home/haram/app/spark-3.5.1-bin-hadoop3/README.md"  # Should be some file on your system

# 1. Session 생성
spark = SparkSession.builder.appName("ParsingParquet").getOrCreate()

# pyspark 에서 multiline(배열) 구조 데이터 읽기
jdf = spark.read.option("multiline","true").json('/home/haram/code/movdata/data/movies/year=2015/data.json')

## companys, directors 값이 다중으로 들어가 있는 경우 찾기 위해 count 컬럼 추가
from pyspark.sql.functions import explode, col, size
edf = jdf.withColumn("company", explode("companys"))

eedf = edf.withColumn("director", explode("directors"))

eedf.write.mode("overwrite").parquet("/home/haram/tmp/parquet_data")


####
spark.stop()