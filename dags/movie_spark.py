from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator
)

DAG_ID = "movie_spark"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="movie spark submit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=["spark", "sbumit", "movie"],
) as dag:
    SPARK_HOME="/home/sgcho/app/spark-3.5.1-bin-hadoop3"
    SCRIPT_BASE="/home/sgcho/code/myairflow/pyspark"
    RAW_BASE="/home/sgcho/data/movie_after/dailyboxoffice"
    META_PATH="/home/sgcho/data/movie_spark/meta"
    

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    def check_exists_meta():
        import os
        if os.path.exists(f'{META_PATH}/_SUCCESS'):
            return append_meta.task_id   
        else:
            return create_meta.task_id
    
    exists_meta = BranchPythonOperator(
        task_id="exists.meta",
        python_callable=check_exists_meta
    )

    append_meta = BashOperator(
        task_id='append.meta', 
        bash_command="""$SPARK_HOME/bin/spark-submit $SCRIPT_BASE/movie_meta.py $RAW_BASE/dt={{ ds_nodash }} append $META_PATH""",
        # bash_command=f"""{SPARK_HOME}/bin/spark-submit {SCRIPT_BASE}/movie_meta.py {RAW_BASE}/dt={{{{ ds_nodash }}}} append {META_PATH}""",
        env={
            "SPARK_HOME": SPARK_HOME, 
            "SCRIPT_BASE": SCRIPT_BASE,
            "META_PATH": META_PATH,
            "RAW_BASE": RAW_BASE
            }
        )
    
    create_meta = BashOperator(
        task_id='create.meta',
        bash_command="""$SPARK_HOME/bin/spark-submit $SCRIPT_BASE/movie_meta.py $RAW_BASE/dt={{ ds_nodash }} create $META_PATH""",
        # bash_command=f"""{SPARK_HOME}/bin/spark-submit {SCRIPT_BASE}/movie_meta.py {RAW_BASE}/dt={{{{ ds_nodash }}}} create {META_PATH}""",
        env={
            "SPARK_HOME": SPARK_HOME, 
            "SCRIPT_BASE": SCRIPT_BASE,
            "RAW_BASE": RAW_BASE,
            "META_PATH": META_PATH
            }
        )
    
    start >> exists_meta >> append_meta >> end
    exists_meta >> create_meta >> end