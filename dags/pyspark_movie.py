from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator

with DAG(
    'pyspark_movie',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    },
    description="transform movie rank 2018.01~04.",
    schedule="10 0 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2016, 1, 1),
    catchup=True,
    tags=["api", "movie","pyspark", "2018"],
) as dag:

    def re(ds_nodash):
        from spark_flow.api import re_partition
        re_partition(ds_nodash)
        print("===============")

    def join(ds_nodash):
        from spark_flow.api import join_df

    task_re_partition=PythonVirtualenvOperator(
            task_id="re.partition",
            python_callable=re,
            requirements=["git+https://github.com/HaramSs/spark_flow.git@0.1.0/spark"]
            )

    join_df = BashOperator(
            task_id='join_df',
            bash_command='''
            echo "spark-submit....."
            $SPARK_HOME/bin/spark-submit /home/haram/code/spark_flow/operator/simple_pyspark.py "JoinDF" {{ ds_nodash }}
            ''',
            )

    task_agg=BashOperator(
            task_id="agg",
            bash_command="""
            echo "agg"
            """
            )
    task_start=EmptyOperator(task_id="start")
    task_end=EmptyOperator(task_id="end")

    task_start >> task_re_partition >> join_df >> task_agg >> task_end

