from pyspark.sql.functions import col, size, explode_outer
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import sys
import os

# LOAD_DT = sys.argv[1]

# logFile = "/home/haram/app/spark-3.5.1-bin-hadoop3/README.md"  # Should be some file on your system

# 1. Session 생성
spark = SparkSession.builder.appName("ParsingParquet").getOrCreate()

# pyspark 에서 multiline(배열) 구조 데이터 읽기
jdf = spark.read.option("multiline","true").json('/home/haram/code/movdata/data/movies/year=2015/data.json')

edf = jdf.withColumn("company", explode_outer("companys"))
eedf = edf.withColumn("director", explode_outer("directors"))

sdf = eedf.select("movieCd", "movieNm", "genreAlt", "typeNm", "director", "company")

sdf = sdf.withColumn("directorNm", col("director.peopleNm"))

rdf = sdf.select("movieCd", "movieNm", "genreAlt", "typeNm", "directorNm", "company.companyCd", "company.companyNm")

rdf.write.mode("overwrite").parquet('/home/haram/tmp/parquet_data')
####
spark.stop()