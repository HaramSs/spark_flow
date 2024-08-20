from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator)

with DAG(
    'dynamic-json',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)

    },
    max_active_tasks=3,
    max_active_runs=1,
    description='movie DAG',
    #schedule_interval=timedelta(days=1),
    schedule="@once",
    start_date=datetime(2015, 1, 1),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

    def tmp():
        pass

    def get_data():
        from movdata.ml import save_movies
        save_movies(2015)

    # def parsing_parquet():
    #     from parsing_parquet import 


    get_data=PythonVirtualenvOperator(
            task_id='get_data',
            python_callable=tmp,
            requirements=["git+https://github.com/HaramSs/movdata.git@ver/0.3"]
            )

    # 배시 오퍼레이터로 만든다.
    # $SPARK_HOME/bin/spark-submit abc.py 로 실행한다.    
    parsing_parquet=BashOperator(
            task_id='parsing_parquet',
            bash_command="""
                $SPARK_HOME/bin/spark-submit /home/haram/code/spark_flow/operator/parsing_parquet.py
            """
             )
    
    select_parquet=BashOperator(
            task_id='select_parquet',
            bash_command="""
                $SPARK_HOME/bin/spark-submit /home/haram/code/spark_flow/operator/select_parquet.py
            """
            )

    start=EmptyOperator(task_id='start')
    end=EmptyOperator(task_id='end')


start >> get_data >> parsing_parquet >> select_parquet >> end
