from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator)

with DAG(
    'line_notify',
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
    tags=['api', 'line', 'notify'],
) as dag:

    bash_job=BashOperator(
            task_id='bash.job',
            bash_command="""
                echo 'bash.job'
                # 0 또는 1을 랜덤하게 생성
                RANDOM_NUM=$RANDOM
                REMAIN=$(( RANDOM_NUM % 3 ))
                echo "RANDOM_NUM:$RANDOM_NUM, REMAIN:$REMAIN"

                if [ $REMAIN -eq 0 ]; then
                echo "작업이 성공했습니다."
                exit 0
                else
                echo "작업이 실패했습니다."
                exit 1
                fi
            """
            )
 
    success_message = "[Notification] Airflow 작업 성공"
    notify_success=BashOperator(
            task_id='notify.success',
            bash_command=f"""
                curl -X POST -H 'Authorization: Bearer zzPPTP8sucp4etFWx4yCx0owzhrSWozaOhKh2rld1oQ' -F 'message={success_message}' \
                https://notify-api.line.me/api/notify
            """
            )
 
    fail_message = "[Notification] Airflow 작업 실패"
    notify_fail=BashOperator(
            task_id='notify.fail',
            bash_command=f"""
                curl -X POST -H 'Authorization: Bearer zzPPTP8sucp4etFWx4yCx0owzhrSWozaOhKh2rld1oQ' -F 'message={fail_message}' \
                https://notify-api.line.me/api/notify
            """,
            trigger_rule='one_failed'
            )
 
    start=EmptyOperator(task_id='start')
    end=EmptyOperator(task_id='end')

    start >> bash_job >> notify_success >> end
    bash_job >> notify_fail >> end
 

 
 