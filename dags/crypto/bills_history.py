from airflow import DAG
from plugins.operators.mysql.init_mysql_operator import InitMysqlOperator
from plugins.operators.data_fetch.okx_fetch_operator import OkxFetchOperator
from include.models.bills_history import BillsHistory
from include.database.mysql_own import engine, db_session
from airflow.models import Variable
from include.okx.Account import AccountAPI


with DAG(
    dag_id='bills_history',
    default_args={'owner': 'Fang'},
    schedule=None,
    tags=['crypto', 'sync']
) as dag:
    config = Variable.get('okx', deserialize_json=True)
    api = AccountAPI(**config).get_account_bills_archive

    init = InitMysqlOperator(
        task_id='init',
        table=BillsHistory,
        engine=engine
    )

    fetch = OkxFetchOperator(
        task_id='fetch',
        api=api
    )

    init >> fetch