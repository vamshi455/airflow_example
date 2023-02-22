from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

new_cluster = {
    'name':'test',
    "spark_version":"7.6.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 0,
    "spark_conf":
    {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master":"local[*]"
    },
    'custom_tags':
    {
        'TeamName': 'DataEngineering'
    }
}
notebook_task_params ={
    'new_cluster': new_cluster,
    'notebook_task': 
    {
        'notebook_path': '/Users/harikrishnaduvvada@gmail.com/airflow-call',
    }
    
}
notebook_task = DatabricksSubmitRunOperator(
    task_id = 'Airflow_adb',
    databricks_conn_id = 'databricks_default',
    dag = dag,
    json=notebook_task_params
)
