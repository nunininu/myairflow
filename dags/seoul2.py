from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

def generate_bash_commands(columns: list):
    cmds = []
    max_length = max(len(c) for c in columns)
    for c in columns:
        # 가변적인 공백 추가 (최대 길이에 맞춰 정렬)
        padding = " " * (max_length - len(c))
        cmds.append(f'echo "{c}{padding} : ====> {{{{ {c} }}}}"')
    return "\n".join(cmds)

local_tz = pendulum.timezone("Asia/Seoul")
    

# Send_notification
# 여기는 파이썬 입니다.
    # 디스코드 noti 보내는 코드를 여기에 넣으면 돌아감
    # "<DAG_ID> <TASK_ID> <YYYYMMDDHH> OK / nunininu" 형식으로
def print_kwargs(dag, task, data_interval_start, **kwargs):
    ds = data_interval_start.in_tz('Asia/Seoul').format('YYYYMMDDHH')
    msg = f"{dag.dag_id} {task.task_id} {ds} OK / nunininu"
    
    from myairflow.send_notification import send_noti
    send_noti(msg)

    # if response.status_code == 204:
    #     print("메시지가 성공적으로 전송되었습니다.")
    # else:
    #     print(f"에러 발생: {response.status_code}, {response.text}")
 
    
##======src/myairflow/send_notification.py로 분리시킴===============##
# def send_noti(msg):
#     import requests
#     import os

#     WEBHOOK_ID = os.getenv('DISCORD_WEBHOOK_ID')
#     WEBHOOK_TOKEN = os.getenv('DISCORD_WEBHOOK_TOKEN')
#     WEBHOOK_URL = f"https://discordapp.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
#     data = { "content": msg }
#     response = requests.post(WEBHOOK_URL, json=data)   
##==================================================================## 

 ##===========================지저분한 방식==========================##
 #def print_kwargs(**kwargs):
    
    # print("kwargs====>", kwargs) ## 위에서 def print_kwargs(**kwargs): 로만 썼다면 아래에 다 써줘야함
    # for k, v in kwargs.items():
    #     print(f"{k}, {v}")
    
    # import requests
    # import os

    # dag_id = kwargs['dag'].dag_id  
    # task_id = kwargs['task'].task_id
    # dis = kwargs['data_interval_start']
    # ds = dis.in_tz('Asia/Seoul').format('YYYYMMDDHH')

    # WEBHOOK_ID = os.getenv('DISCORD_WEBHOOK_ID')
    # WEBHOOK_TOKEN = os.getenv('DISCORD_WEBHOOK_TOKEN')
    # WEBHOOK_URL = f"https://discordapp.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    # data = { "content": f"{dag_id} {task_id} {ds} OK / nunininu" }
    # response = requests.post(WEBHOOK_URL, json=data)

    # if response.status_code == 204:
    #         print("메시지가 성공적으로 전송되었습니다.")
    # else:
    #         print(f"에러 발생: {response.status_code}, {response.text}")
    
 ##==================================================================##   
    
# Directed Acyclic Graph
with DAG(
    "seoul2",
    schedule="@hourly",
    # start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul")
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=print_kwargs
    )

 
    columns_b1 = [
    "data_interval_start", "data_interval_end", "logical_date", "ds", "ds_nodash",
    # "exception",
    "ts", "ts_nodash_with_tz", "ts_nodash", "prev_data_interval_start_success",
    "prev_data_interval_end_success", "prev_start_date_success", "prev_end_date_success",
    "inlets", "inlet_events", "outlets", "outlet_events", "dag", "task", "macros",
    "task_instance", "ti", "params", "var.value", "var.json", "conn", "task_instance_key_str",
    "run_id", "dag_run", "map_index_template", "expanded_ti_count", "triggering_dataset_events"
    ]
    cmds_b1 = generate_bash_commands(columns_b1)
   
    b1 = BashOperator(
        task_id="b_1", 
        bash_command=f"""
            echo "date ====================> `date`"
            {cmds_b1}
        """)
    
    cmds_b2_1 = [
    "execution_date",
    "next_execution_date","next_ds","next_ds_nodash",
    "prev_execution_date","prev_ds","prev_ds_nodash",
    "yesterday_ds","yesterday_ds_nodash",
    "tomorrow_ds", "tomorrow_ds_nodash",
    "prev_execution_date_success",
    "conf"
    ]
    
    cmds_b2_1 = generate_bash_commands(cmds_b2_1)
   
    b2_1 = BashOperator(task_id="b_2_1", 
                        bash_command=f"""
                        {cmds_b2_1}
                        """)
    
    b2_2 = BashOperator(task_id="b_2_2", 
                        bash_command="""
                        echo "data_interval_start : {{ data_interval_start.in_tz('Asia/Seoul') }}"
                        """)
    
    mkdir = BashOperator(
        task_id="mkdir",
        bash_command="""
        execution_date_kst="{{ execution_date.in_tz('Asia/Seoul') }}"
        year=$(date -d "$execution_date_kst" +%Y)
        month=$(date -d "$execution_date_kst" +%m)
        day=$(date -d "$execution_date_kst" +%d)
        hour=$(date -d "$execution_date_kst" +%H)
        mkdir -p ~/data/seoul/$year/$month/$day/$hour
        """
    )
    
    start >> b1 >> [b2_1, b2_2] >> mkdir >> end
    mkdir >> send_notification
    
    # start >> b1
    # b1 >> [b2_1, b2_2]
    # [b2_1, b2_2] >> end
    
    # start >> b1 >> b2_1
    # b1 >> b2_2
    # [b2_1, b2_2] >> end
    
if __name__ == "__main__":
    dag.test()
