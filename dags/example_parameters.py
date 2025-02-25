# 执行sql文件
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta


with DAG(
    'example_parameters',
    default_args={'owner': 'Fang', 'retries': 1},
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    catchup=False,
) as dag:
    mission = MySqlOperator(
        task_id='example_parameters',
        sql='sql/example_parameters.sql',
        mysql_conn_id='mysql_default',
        autocommit=True,
    )

    mission
