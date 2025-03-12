from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
import pendulum

# Send_notification
# 여기는 파이썬 입니다.
    # 디스코드 noti 보내는 코드를 여기에 넣으면 돌아감
# def print_kwargs():
#     # ds = data_interval_start.in_tz('Asia/Seoul').format('YYYYMMDDHH')
#     import time
#     import random
#     snum = random.randint(1,10)
#     time.sleep(snum)
#     print(snum)

# def print_kwargs():
#     from myairflow.send_notification import send_noti
#     send_noti("python virtualenv operator: nunininu")


    
# Directed Acyclic Graph
with DAG(
    "virtual",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
    default_args={
        "depends_on_past": False
        },
    # max_active_runs=1
    max_active_runs=1
    ) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # def f_vpython():
    #     from myairflow.send_notification import send_noti
    #     send_noti("python virtualenv operator: nunininu / vpython")
    
    def f_vpython(dis):
        from myairflow.send_notification import send_noti
        send_noti(f"time {dis}: nunininu / vpython")
        
    
    def f_python(**kwargs):
        from myairflow.send_notification import send_noti
        ti = kwargs['data_interval_start'].in_tz('Asia/Seoul').format('YYYYMMDDHH')
        send_noti(f"time {ti}: nunininu / python")
    
    # send_notification = PythonVirtualenvOperator(
    #     task_id="send_notification",
    #     python_callable=print_kwargs,
    #     requirements=[
    #         "git+https://github.com/nunininu/myairflow.git@0.1.0"
    #     ]
    
    t_python = PythonOperator(
        task_id="t_python", python_callable=f_python
        )
    
    
    t_vpython = PythonVirtualenvOperator(
            task_id="t_vpython", python_callable=f_vpython, 
            requirements=["git+https://github.com/nunininu/myairflow.git@0.1.0"],
            op_args=["{{ data_interval_start.in_tz('Asia/Seoul').format('YYYYMMDDHH') }}"],
            provide_context=True
        )
        
    
 
    start >> t_vpython >> t_python >> end

    
    
if __name__ == "__main__":
    dag.test()
