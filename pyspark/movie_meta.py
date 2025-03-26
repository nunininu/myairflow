from pyspark.sql import SparkSession, DataFrame
import sys
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)] # stdout으로 출력
)

spark = SparkSession.builder.appName("movie_meta").getOrCreate()

exit_code = 0  #0이면 정상 종료, 1이면 비정상 종료

def save_meta(df: DataFrame, meta_path: str):
    df.write.mode("overwrite").parquet(meta_path)
    logging.info("meta 저장 완료", meta_path)


# try:
    # {{ds_nodash}} append $META PATH
    # if len(sys.argv) == 4:
    #     load_dt = sys.argv[1]
    #     mode = sys.argv[2]
    #     meta_path = sys.argv[3]
    # else:
    #     exit_code = 1
    #     raise ValueError("필수 인자가 누락되었습니다")

try:
    if len(sys.argv) != 4:   #위의 else 있는 조건문 대신 else 없앤 간소화 버전
            exit_code = 1
            raise ValueError("필수 인자가 누락되었습니다")
            
    # load_dt = sys.argv[1]
    # mode = sys.argv[2]
    # meta_path = sys.argv[3]
    
    # load_dt, mode, meta_path = sys.argv[1:4]  #위 3줄을 한 줄 짜리로 줄인 코드
    
    raw_path, mode, meta_path = sys.argv[1:4]
    
    # /home/sgcho/data/movie_after/dailyboxoffice/dt={load_dt}를 Airflow DAG에서 받아오도록 수정 (하드코딩 배제)
    meta_today = spark.read.parquet(raw_path).select("movieCd", "multiMovieYn", "repNationCd")
    
    if mode == "create":
        save_meta(meta_today, meta_path)
    elif mode == "append":
        meta_yesterday = spark.read.parquet(meta_path)
        meta_yesterday.createOrReplaceTempView("temp_meta_yesterday")
        meta_today.createOrReplaceTempView("temp_meta_today")
        
        updated_meta = spark.sql("""
            SELECT 
                COALESCE(y.movieCd, t.movieCd) AS movieCd,
                COALESCE(y.multiMovieYn, t.multiMovieYn) AS multiMovieYn,
                COALESCE(y.repNationCd, t.repNationCd) AS repNationCd
            FROM temp_meta_yesterday y
            FULL OUTER JOIN temp_meta_today t
            ON y.movieCd = t.movieCd""")
        updated_meta.createOrReplaceTempView("temp_meta_update")
        
        # ASSERT_TRUE - updated meta 는 항상 원천 소스 보다 크면 정산 / else 실패
        spark.sql("""
        SELECT ASSERT_TRUE(
            (SELECT COUNT(*) FROM temp_meta_update) > (SELECT COUNT(*) FROM temp_meta_yesterday) AND
            (SELECT COUNT(*) FROM temp_meta_update) > (SELECT COUNT(*) FROM temp_meta_today)
        ) AS is_valid
        
        /*
        WITH counts AS (
            SELECT 
                (SELECT COUNT(*) FROM temp_meta_update) AS count_update,
                (SELECT COUNT(*) FROM temp_meta_yesterday) AS count_yesterday,
                (SELECT COUNT(*) FROM temp_meta_today) AS count_today
        )
        SELECT ASSERT_TRUE(
            count_update > count_yesterday AND count_update > count_today
        ) AS is_valid
        FROM counts
        */
        """)
        
        save_meta(updated_meta, meta_path)
    else:
        raise ValueError(f"알 수 없는 MODE: {mode}")
    
    print("*"*33) #작동 확인용 코드
    print("DONE") #작동 확인용 코드
    logging.debug("I DEBUG") #작동 확인용 코드
    logging.info("I DONE") #작동 확인용 코드
    logging.warning("WARNING") #작동 확인용 코드
    logging.error("ERROR") #작동 확인용 코드
    logging.critical("CRITICAL") #작동 확인용 코드
    print("*"*33) #작동 확인용 코드
            
except Exception as e:
    logging.error(f"오류 : {str(e)}")
    exit_code = 1

finally:
    spark.stop()
    sys.exit()
    