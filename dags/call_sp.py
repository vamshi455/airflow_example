import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2021,3,22,17,15)}

dag = DAG(
    dag_id="calling_sp", default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 19)
}, schedule_interval=None
)


with dag:
    query1_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql="CALL DARE.PUBLIC.check_sp()",
        snowflake_conn_id="DARE_DB_SNOWFLAKE",
    )
