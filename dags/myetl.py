# from datetime import datetime, timedelta
# from airflow.models.dag import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
# import pendulum
# import pandas as pd

# # Directed Acyclic Graph
# with DAG(
#     "myetl",
#     schedule="@hourly",
#     start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
#     default_args={
#         "depends_on_past": False
#         },
#     # max_active_runs=1
#     max_active_runs=1
#     ) as dag:
    
#     start = EmptyOperator(task_id="start")
#     end = EmptyOperator(task_id="end")

#     make_data = BashOperator(
#         task_id="make_data", 
#         bash_command="""
#         /home/sgcho/airflow/make_data.sh /home/sgcho/data/2025/03/12/01
#         # execution_date_kst="{{ execution_date.in_tz('Asia/Seoul') }}"
#         # year=$(date -d "$execution_date_kst" +%Y)
#         # month=$(date -d "$execution_date_kst" +%m)
#         # day=$(date -d "$execution_date_kst" +%d)
#         # hour=$(date -d "$execution_date_kst" +%H)
#         # mkdir -p ~/data/sgcho/$year/$month/$day/$hour
#         """
#         )
    
#     def f_load_data():
#         data = pd.read_csv("/home/sgcho/data/2025/03/12/01/data.csv")
#         df = pd.DataFrame(data)   
#         # DataFrame을 Parquet 파일로 저장
#         df.to_parquet('data.parquet', engine='pyarrow')

#         # Parquet 파일을 읽어 DataFrame으로 로드
#         df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')

#         # 출력
#         print(df_loaded)
    
#     def f_agg_data():
        
    
#         load_data = PythonVirtualenvOperator(
#             task_id="load_data", python_callable=f_load_dat, 
            
            
#         )
    
#         agg_data = PythonVirtualenvOperator(
#             task_id="agg_data", python_callable=f_agg_data, 
            
            
#         )

    
    




# start >> make_data >> load_data >> agg_data >> end


# # 샘플 DataFrame 생성
# data = {
#     'id': [1, 2, 3, 4, 5],
#     'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
#     'value': [100, 200, 300, 400, 500]
# }

# df = pd.DataFrame(data)

# # DataFrame을 Parquet 파일로 저장
# df.to_parquet('data.parquet', engine='pyarrow')

# # Parquet 파일을 읽어 DataFrame으로 로드
# df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')

# # 출력
# print(df_loaded)






