# 执行sql文件
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta


with DAG(
    'example_simple_sql',
    default_args={'owner': 'Fang', 'retries': 1},
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    catchup=False,
) as dag:
    mission = MySqlOperator(
        task_id='execute_simple_sql',
        sql='sql/example_simple_sql.sql',
        mysql_conn_id='mysql_default',
        autocommit=True,
    )

    mission
