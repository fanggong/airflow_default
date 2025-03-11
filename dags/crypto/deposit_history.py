from airflow import DAG
from plugins.operators.mysql.init_mysql_operator import InitMysqlOperator
from plugins.operators.data_fetch.okx_fetch_operator import OkxFetchOperator
from include.models.deposit_history import DepositHistory
from include.database.mysql_own import engine, db_session
from airflow.models import Variable
from include.okx.Funding import FundingAPI


with DAG(
    dag_id='deposit_history',
    default_args={'owner': 'Fang'},
    schedule=None,
    tags=['crypto', 'sync']
) as dag:
    config = Variable.get('okx', deserialize_json=True)
    api = FundingAPI(**config).get_deposit_history

    init = InitMysqlOperator(
        task_id='init',
        table=DepositHistory,
        engine=engine
    )

    fetch = OkxFetchOperator(
        task_id='fetch',
        api=api
    )

    init >> fetch