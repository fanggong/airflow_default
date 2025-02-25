# 执行sql文件
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta


with DAG(
    'example_internal_dependency',
    default_args={'owner': 'Fang', 'retries': 1},
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    catchup=False,
) as dag:
    ods2dwd = MySqlOperator(
        task_id='ods2dwd',
        sql='sql/example_simple_sql.sql',
        mysql_conn_id='mysql_default',
        autocommit=True,
    )

    dwd2dws = MySqlOperator(
        task_id='dwd2dws',
        sql='sql/example_simple_sql.sql',
        mysql_conn_id='mysql_default',
        autocommit=True,
    )

    ods2dwd >> dwd2dws
