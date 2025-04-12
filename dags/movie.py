from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)

with DAG(
    'movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['api', 'movie'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/nunininu/movie.git@250322.0"]
    BASE_DIR = "~/data/movies/dailyboxoffice"
    import os
    
    def branch_fun(ds_nodash, BASE_DIR):
        
        check_path = os.path.expanduser(f"{BASE_DIR}/dt={ds_nodash}") #버그 수정
        if os.path.exists(check_path): #버그 수정
            return rm_dir.task_id #task_id로 리턴하는 방법
            #return "rm.dir" #task실제이름으로 리턴하는 방법
        else:
            return "get.start", "echo.task" #task실제이름으로 리턴하는 방법

    # ERR: NameError (BASE_DIR is not defined)
    # 추가 설명: 
    # """
    # branch_fun 함수의 정의를 def branch_fun(ds_nodash, base_dir):로 수정하여 base_dir 인자를 받도록 합니다.
    # BranchPythonOperator의 op_kwargs를 사용하여 {"base_dir": BASE_DIR}을 전달합니다. 
    # 이렇게 하면 BASE_DIR 값이 branch_fun 함수 내부의 base_dir 변수에 전달됩니다.
    # 이렇게 수정하면 branch_fun 함수 내부에서 BASE_DIR을 사용할 수 있게 되어 NameError가 해결됩니다.
    # """
        
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
        op_kwargs={"BASE_DIR": BASE_DIR} # BASE_DIR을 인자로 전달
    )
    
    def fn_merge_data(ds_nodash, BASE_DIR):
        from movie.api.call import fill_na_with_column, gen_unique_df, re_ranking, save_df
        import pandas as pd
        #print(ds_nodash)
        # df read => ~/data/movies/dailyboxoffice/dt=20240101
        path = f"{BASE_DIR}/dt={ds_nodash}"
        df = pd.read_parquet(path)
        df1 = fill_na_with_column(df, 'multiMovieYn')
        df2 = fill_na_with_column(df1, 'repNationCd')
        drop_columns=['rnum', 'rank', 'rankInten', 'salesShare']
        unique_df = gen_unique_df(df=df2, drop_columns=drop_columns)
        new_ranked_df = re_ranking(unique_df, dt=ds_nodash)
        merge_save_path = save_df(new_ranked_df, f"/home/sgcho/data/movies/merge/dailyboxoffice")
        print(merge_save_path + "<================ 에 저장됨")
        
        
        
        
        
        
        # df1 = fill_na_with_column(df, 'multiMovieYn')
        # df2 = fill_na_with_column(df1, 'repNationCd')
        # df3 = df2.drop(columns=['rnum', 'rank', 'rankInten', 'salesShare'])
        # unique_df = df3.drop_duplicates() # 25
        # unique_df.loc[:, "rnum"] = unique_df["audiCnt"].rank(ascending=False).astype(int)
        # unique_df.loc[:, "rank"] = unique_df["audiCnt"].rank(ascending=False).astype(int)
        # save -> ~/data/sgcho/movies/merge/dailyboxoffice/dt=20240101
            
    merge_data = PythonVirtualenvOperator(
        task_id='merge.data',
        python_callable=fn_merge_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"BASE_DIR": BASE_DIR} # BASE_DIR을 인자로 전달
    )

    
    BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    KEY=os.getenv("MOVIE_KEY")

    def common_get_data(ds_nodash, url_param, base_path):
        # TODO
        # API로 불러온 데이터를 
        # BASE_DIR/dt=20240101/repNationCd=K/****.parquet
        # STEP 1 - GIT에서 PIP를 설치하고
        # BASE_DIR/dt=202040101/ 먼저 해서 잘 되면
        # repNationCd=k/도 붙여준다
        # 그 후 다른 것도 붙여준다
        from movie.api.call import call_api, list2df, save_df 
        print(ds_nodash, url_param, base_path)
        
        # From Test_call.py 
        data = call_api(ds_nodash, url_param=url_param)   # def test_save_df_url_params() 에서 가져온 코드 응용
        df = list2df(data, ds_nodash, url_param)             # def test_save_df_url_params() 에서 가져온 코드 응용
        partitions = ['dt'] + list(url_param.keys())   # def test_save_df_url_params() 에서 가져온 코드 응용
        save_path = save_df(df, base_path, partitions)          # def test_save_df_url_params() 에서 가져온 코드 응용
            
        
        # Airflow의 log 메세지 숨김/펼침 기능 포맷 
        # https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html#grouping-of-log-lines
        print("::group::movie df save...")
        print("save_path--->"+ save_path)
        print("url_param--->" + str(url_param))
        print("ds_nodash--->" + ds_nodash)
        print("::endgroup::")
        
        print(save_path, url_param)
        
        ## 구 코드
        # data = call_api(ds_nodash, url_param) # def test_save_df 에서 가져옴
        # df = list2df(data, ds_nodash, url_param)        # def test_save_df 에서 가져옴
        # save_path = save_df(df, base_path) # def test_save_df 에서 가져옴
        
        
        # def test_save_df():
        #     ymd = "20210101"
        #     data = call_api(dt=ymd)
        #     df = list2df(data, ymd)
        #     base_path = "~/temp/movie"
        #     r = save_df(df, base_path)
        #     assert r == f"{base_path}/dt={ymd}"
        #     print("save_path", r)
        #     read_df = pd.read_parquet(r)
        #     assert 'dt' not in read_df.columns
        #     assert 'dt' in pd.read_parquet(base_path).columns
        
        
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"multiMovieYn": "Y"},"base_path": BASE_DIR}
        )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"multiMovieYn": "N"},"base_path": BASE_DIR}
        )
    

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"repNationCd": "K"},"base_path": BASE_DIR}
        )
    

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"repNationCd": "F"},"base_path": BASE_DIR}
        )
    
    no_param = PythonVirtualenvOperator(
        task_id='no.param',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{},"base_path": BASE_DIR}
        )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command=f'rm -rf {BASE_DIR}' + '/dt={{ ds_nodash }}') #버그 수정
                          

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    get_start = EmptyOperator(task_id='get.start',
                              trigger_rule="all_done")
    get_end = EmptyOperator(task_id='get.end')


    make_done = BashOperator(
        task_id = 'make.done',
        bash_command="""
        DONE_BASE=/home/sgcho/data/movies/done/dailyboxoffice
        mkdir -p ${DONE_BASE}/{{ ds_nodash }}
        touch ${DONE_BASE}/{{ ds_nodash }}/_DONE
        """
    )    

    start >> branch_op

    branch_op >> rm_dir >> get_start
    branch_op >> get_start
    branch_op >> echo_task
    get_start >> [multi_y, multi_n, nation_k, nation_f, no_param] >> get_end

    get_end >> merge_data >> make_done >> end










## 내가 썼던 코드 
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.models.dag import DAG
# from airflow.operators.python import (
#         BranchPythonOperator, 
#         PythonVirtualenvOperator,
# )

# # # Directed Acyclic Graph
# with DAG(
#     "merge_data"
#     # schedule="@hourly",
#     # start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
# ) as dag:
    
#     start = EmptyOperator(task_id="start")
        
#     def dummy_fun():
#         pass
    
#     branch_op = BranchPythonOperator(
#         task_id="branch.op",
#         python_callable=dummy_fun
#     )
    
#     rm_dir = BashOperator(
#         task_id="rm.dir", 
#         bash_command=""
#     )
    
#     echo_task = BashOperator(
#         task_id="echo.task", 
#         bash_command=""
#     )
    
#     get_start = EmptyOperator(task_id="get.start")
    
    
#     no_param = PythonVirtualenvOperator(
#         task_id="no.param",
#         python_callable=dummy_fun
#     )
    
#     multi_n = PythonVirtualenvOperator(
#         task_id="multi.n",
#         python_callable=dummy_fun
#     )
    
#     multi_y = PythonVirtualenvOperator(
#         task_id="multi.y",
#         python_callable=dummy_fun
#     )
    
#     nation_f = PythonVirtualenvOperator(
#         task_id="nation.f",
#         python_callable=dummy_fun
#     )
    
#     nation_k = PythonVirtualenvOperator(
#         task_id="nation.k",
#         python_callable=dummy_fun
#     )
    
#     get_end = EmptyOperator(task_id="get.end")
    
#     merge_data = BranchPythonOperator(
#         task_id="merge.data",
#         python_callable=dummy_fun
#     )
    
#     end = EmptyOperator(task_id="end")
    

    
# start >> branch_op >> [rm_dir, echo_task] >> get_start
# get_start >> [no_param, multi_n, multi_y, nation_f, nation_k]
# [no_param, multi_n, multi_y, nation_f, nation_k] >> get_end >> merge_data >> end



# get.end >> merge.data >> end
# echo.task
# [rm.dir, echo.task]
# rm.dir >> get.start >> no.param >> 
# >> no.param
# >> multi.n
# >> multi.y
# >> nation.f
# >> nation.k

