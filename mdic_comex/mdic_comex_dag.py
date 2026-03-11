
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SPARK_APP_PATH = "/user/cloudera/airflow_scripts/mdic_comex_crawler.py"
SPARK_CONFIG_PATH = "/user/cloudera/airflow_scripts/mdic_comex_config.json"

# Configurações do Spark (ajuste conforme seu ambiente Cloudera)
SPARK_MASTER = "yarn"
SPARK_DEPLOY_MODE = "cluster" # ou client, dependendo da configuração do seu cluster
SPARK_EXECUTOR_MEMORY = "4g"
SPARK_DRIVER_MEMORY = "2g"
SPARK_NUM_EXECUTORS = "4"

with DAG(
    dag_id="mdic_comex_crawler_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # Executa diariamente
    catchup=False,
    tags=["mdic", "comex", "spark", "hdfs", "crawler"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    submit_spark_job = BashOperator(
        task_id="run_mdic_comex_crawler",
        bash_command=f"""
            spark-submit \
            --master {SPARK_MASTER} \
            --deploy-mode {SPARK_DEPLOY_MODE} \
            --executor-memory {SPARK_EXECUTOR_MEMORY} \
            --driver-memory {SPARK_DRIVER_MEMORY} \
            --num-executors {SPARK_NUM_EXECUTORS} \
            {SPARK_APP_PATH} {SPARK_CONFIG_PATH}
        """,
    )
