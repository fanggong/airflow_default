from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from include.service import init_mysql, okx_fetch, sync_mysql
from include.models.bills_history import BillsHistory
from include.utils.utils import from_timestamp, process_keys
from include.database.mysql_own import engine, db_session
from airflow.models import Variable
from include.okx.Account import AccountAPI
from datetime import datetime


@dag(schedule_interval='*/30 * * * *', default_args={'owner': 'Fang'}, tags=['crypto', 'sync'],
     start_date=datetime(2023, 1, 1), catchup=False)
def test():
    
    @task
    def get_period(**kwargs):
        start_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        return {'start_time': int(start_time.timestamp()*1000), 'end_time': end_time}

    get_period()

test()