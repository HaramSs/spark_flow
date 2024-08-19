from pyspark.sql import SparkSession
import sys

app_name = sys.argv[1]
load_dt = sys.argv[2]

spark = SparkSession.builder.appName(Simple_sum).getOrCreate()

df1 = spark.read.parquet(f"/home/haram/data/movie/hive/load_dt={LOAD_DT}")

df1.createOrReplaceTempView("one_day")

df_n = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt
