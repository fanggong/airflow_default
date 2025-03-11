from airflow import DAG
from plugins.operators.mysql.init_mysql_operator import InitMysqlOperator
from plugins.operators.data_fetch.okx_fetch_operator import OkxFetchOperator
from plugins.operators.mysql.sync_mysql_operator import SyncMysqlOperator
from airflow.operators.python import PythonOperator
from include.models.balance import Balance
from include.utils import from_timestamp, process_keys
from include.database.mysql_own import db_session, engine
from include.okx.Account import AccountAPI
from airflow.models import Variable

def process_item(item):
    key_mapping = {
        'accAvgPx': 'acc_avg_px',
        'availBal': 'avail_bal',
        'availEq': 'avail_eq',
        'borrowFroz': 'borrow_froz',
        'cashBal': 'cash_bal',
        'ccy': 'ccy',
        'clSpotInUseAmt': 'cl_spot_in_use_amt',
        'crossLiab': 'cross_liab',
        'disEq': 'dis_eq',
        'eq': 'eq',
        'eqUsd': 'eq_usd',
        'fixedBal': 'fixed_bal',
        'frozenBal': 'frozen_bal',
        'imr': 'imr',
        'interest': 'interest',
        'isoEq': 'iso_eq',
        'isoLiab': 'iso_liab',
        'isoUpl': 'iso_upl',
        'liab': 'liab',
        'maxLoan': 'max_loan',
        'maxSpotInUse': 'max_spot_in_use',
        'mgnRatio': 'mgn_ratio',
        'mmr': 'mmr',
        'notionalLever': 'notional_lever',
        'openAvgPx': 'open_avg_px',
        'ordFrozen': 'ord_frozen',
        'rewardBal': 'reward_bal',
        'smtSyncEq': 'smt_sync_eq',
        'spotBal': 'spot_bal',
        'spotInUseAmt': 'spot_in_use_amt',
        'spotIsoBal': 'spot_iso_bal',
        'spotUpl': 'spot_upl',
        'spotUplRatio': 'spot_upl_ratio',
        'stgyEq': 'stgy_eq',
        'totalPnl': 'total_pnl',
        'totalPnlRatio': 'total_pnl_ratio',
        'twap': 'twap',
        'uTime': 'u_time',
        'upl': 'upl',
        'uplLiab': 'upl_liab'
    }
    item['uTime'] = from_timestamp(item['uTime'])
    processed_item = process_keys(item, key_mapping)
    return processed_item

def process_data(**context):
    fetched_data = context['task_instance'].xcom_pull(task_ids='fetch', key='return_value')
    if not fetched_data:
        raise ValueError("未获取到数据！")
    processed_data = fetched_data['data'][0]['details']
    processed_data = [process_item(item) for item in processed_data]
    return processed_data 


with DAG(
    dag_id='balance',
    schedule=None,
    default_args={'owner': 'Fang'},
    tags=['crypto', 'sync']
) as dag:
    config = Variable.get('okx', deserialize_json=True)
    api = AccountAPI(**config).get_account_balance

    init = InitMysqlOperator(
        task_id='init', 
        table=Balance,
        engine=engine
    )
    fetch = OkxFetchOperator(
        task_id='fetch',
        api=api
    )
    process = PythonOperator(
        task_id='process',
        python_callable=process_data
    )
    sync = SyncMysqlOperator(
        task_id='sync', 
        table=Balance,
        session=db_session,
        type='full'
    )

    init >> fetch >> process >> sync