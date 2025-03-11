from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

# Directed Acyclic Graph
with DAG(
    "seoul",
    # schedule="@hourly",
    schedule="0 * * * *",
    # start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul")
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    b1 = BashOperator(
        task_id="b_1", 
        bash_command="""
            echo "date ====================> `date`"
            echo "data_interval_start =====> {{ data_interval_start }}"
            echo "data_interval_end =======> {{ data_interval_end }}"
            echo "logical_date  =======> {{ logical_date  }}"
            echo "ds   =======> {{ ds }}"
            echo "ds   =======> {{ ds }}"
        """)
    b2_1 = BashOperator(task_id="b_2_1", bash_command="echo 2_1")
    b2_2 = BashOperator(task_id="b_2_2", bash_command="echo 2_2")
    
    start >> b1 >> [b2_1, b2_2] >> end
    
    # start >> b1
    # b1 >> [b2_1, b2_2]
    # [b2_1, b2_2] >> end
    
    # start >> b1 >> b2_1
    # b1 >> b2_2
    # [b2_1, b2_2] >> end
    