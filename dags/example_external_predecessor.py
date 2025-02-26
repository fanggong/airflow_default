from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago


with DAG(
    'example_external_predecessor',
    default_args={'owner': 'Fang', 'retries': 1},
    schedule_interval='0 */2 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    ods2dwd = MySqlOperator(
        task_id='ods2dwd',
        sql='sql/ods2dwd.sql',
        mysql_conn_id='mysql_default',
        autocommit=True,
    )
    ods2dwd