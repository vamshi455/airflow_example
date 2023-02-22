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
    dag_id="snowflake_connector3", default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 19)
}, schedule_interval=None
)

query1 = [
    """select 1;""",
    """SHOW TABLES HISTORY IN DARE.public;""",
]


# def count1(**context):
#     dwh_hook = SnowflakeHook(snowflake_conn_id="DARE_DB_SNOWFLAKE")
#     result = dwh_hook.get_first("select count(*) from DARE.PUBLIC.EMPLOYEES")
#     logging.info("Number of rows in `DARE.PUBLIC.EMPLOYEES`  - %s", result[0])

# def listTables(**context):
#     dwh_hook = SnowflakeHook(snowflake_conn_id="DARE_DB_SNOWFLAKE")
#     result = dwh_hook.get_first("SHOW TABLES HISTORY IN DARE.public")
#     logging.info("Number of tables in `DARE.public`  - %s", result)

def call_incSP(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="DARE_DB_SNOWFLAKE")
    result = dwh_hook.get_first("CALL DARE.PUBLIC.check_sp()")
    logging.info("calling sp_inc_load Sp in `DARE.public`  - %s", result)


with dag:
    query1_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql=query1,
        snowflake_conn_id="DARE_DB_SNOWFLAKE",
    )

    # count_query = PythonOperator(task_id="count_query", python_callable=count1)
    # list_query = PythonOperator(task_id="list_tables", python_callable=listTables)
    call_sp = PythonOperator(task_id="call_sp", python_callable=call_incSP)
call_sp# >> count_query
#count_query >> list_query