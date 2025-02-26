from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago


with DAG(
    'example_external_task',
    default_args={'owner': 'Fang', 'retries': 1},
    schedule_interval='0 */2 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    predecessor = ExternalTaskSensor(
        task_id='ods2dwd_ex',
        external_dag_id='example_external_predecessor',  # 等待dag1
        external_task_id='ods2dwd',  # 等待task1
        allowed_states=['success'],  # 只在task1成功后继续
        failed_states=['failed', 'skipped'],  # 任务失败或跳过时不再继续
        poke_interval=10,  # 每隔60秒检查一次
        timeout=600,  # 最多等待10分钟
    )

    dwd2dws = MySqlOperator(
        task_id='dwd2dws',
        sql='sql/dwd2dws.sql',
        mysql_conn_id='mysql_default',
        autocommit=True,
    )
    
    predecessor >> dwd2dws