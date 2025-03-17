from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from include.models.bills_history import BillsHistory
from include.utils.utils import from_timestamp, process_keys
from include.database.mysql_own import engine, db_session
from airflow.models import Variable
from include.okx.Account import AccountAPI
from datetime import datetime


@dag(schedule=None, default_args={'owner': 'Fang'}, tags=['crypto', 'sync'])
def test():
    
    @task
    def get_period(**kwargs):
        return kwargs['ds_nodash']

    get_period()

test()