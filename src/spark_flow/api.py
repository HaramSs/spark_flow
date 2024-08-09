import pandas as pd
import os
import shutil

def re_partition(load_dt, from_path='/data/movie/extract'):
    home_dir = os.path.expanduser("~") # /Home/HaramSs

    # /Home/HaramSs/data/movie/extract
    read_path = os.path.join(home_dir, 'data','movie','extract', f'load_dt={load_dt}')
    # /home/haram/data/movie/extract
    # /home/haram/data/movie/extract/load_dt=20150101
    write_path = f'{home_dir}/data/movie/repartition'

    df = pd.read_parquet(read_path)

    # DataFrame에 "load_dt" 컬럼을 생성 load_dt컬럼에는 load_dt값이 들어간다.
    df['load_dt'] = load_dt

    # df의 DataFrame을 wirte_path에 parquet파일로 생성 
    # 그리고 파티셔닝은 load_dt, multiMovieYn, repNationCd 기준으로
    df.to_parquet(
            write_path,
            partition_cols=['load_dt', 'multiMovieYn', 'repNationCd']
            )
    
    # 결과 반환은 df의 길이 (row 수), 파일을 읽는 경로, 파일을 쓰는 경로
    return len(df), read_path, f'{write_path}/load_dt={load_dt}'

def rm_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)

def join_df(load_dt, from_path=''):
    pass
