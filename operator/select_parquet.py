from pyspark.sql import SparkSession
import sys
import os
import json

spark = SparkSession.builder.appName("SelectParquet").getOrCreate()

read_df = spark.read.parquet('/home/haram/tmp/parquet_data')

read_df.createOrReplaceTempView("movdir")

sql = spark.sql("""
SELECT directorNm, COUNT(*) AS cnt FROM movdir 
GROUP BY directorNm 
-- HAVING cnt > 3
ORDER BY cnt DESC 
LIMIT 10
""")

sql.show()

####
spark.stop()

