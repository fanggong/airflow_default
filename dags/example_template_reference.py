from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta


with DAG(
    'example_template_reference',
    default_args={'owner': 'Fang', 'retries': 1},
    schedule_interval='0 */2 * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:
    mission = MySqlOperator(
        task_id='example_template_reference',
        sql='sql/example_template_reference.sql',
        mysql_conn_id='mysql_default',
        autocommit=True,
    )

    mission
