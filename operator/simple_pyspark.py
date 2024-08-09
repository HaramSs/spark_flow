from pyspark.sql import SparkSession
import sys

app_name = sys.argv[1]
LOAD_DT = sys.argv[2]

logFile = "/home/haram/app/spark-3.5.1-bin-hadoop3/README.md"  # Should be some file on your system

# 1. Session 생성
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

# 2. 데이터를 읽어 와야한다.
df1 = spark.read.parquet(f"/home/haram/data/movie/repartition/load_dt={LOAD_DT}")

# 3. 읽어온 데이터를 임시 뷰로 생성
df1.createOrReplaceTempView("one_day")

# 4. 읽어온 데이터를 가공
df2 = spark.sql(f"""
SELECT 
    movieCd, -- 영화의 대표코드
    repNationCd -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
FROM one_day
WHERE multiMovieYn IS NULL
""")
df2.createOrReplaceTempView("multi_null")

df3 = spark.sql(f"""
SELECT 
    movieCd, -- 영화의 대표코드
    multiMovieYn -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
FROM one_day
WHERE repNationCd IS NULL
""")
df2.createOrReplaceTempView("nation_null")



df2 = spark.sql(f"""
SELECT 
    movieCd, -- 영화의 대표코드
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt, --- 사영횟수
    -- multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM one_day
WHERE multiMovieYn IS NULL
""")

df2.createOrReplaceTempView("multi_null")

df3 = spark.sql(f"""
SELECT 
    movieCd, -- 영화의 대표코드
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt, --- 사영횟수
    multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    -- repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM one_day
WHERE repNationCd IS NULL
""")

df3.createOrReplaceTempView("nation_null")

df_j = spark.sql(f"""
SELECT
    COALESCE(m.movieCd, n.movieCd) AS movieCd,
    COALESCE(m.salesAmt, n.salesAmt) as totalSalesAmt, -- 매출액
    COALESCE(m.audiCnt, n.audiCnt) as totalAudiCnt, -- 관객수
    COALESCE(m.showCnt, n.showCnt) as totalShowCnt, --- 사영횟수
    multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM multi_null m FULL OUTER JOIN nation_null n
ON m.movieCd = n.movieCd""")

df_j.createOrReplaceTempView("join_df")

df_j.write.mode("overwrite").partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/haram/data/movie/hive")
df_j.show()

####
spark.stop()

