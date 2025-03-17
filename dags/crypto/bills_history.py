from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from include.service import init_mysql, okx_fetch, write_to_mysql
from include.models.bills_history import BillsHistory
from include.utils.utils import from_timestamp, process_keys
from include.database.mysql_own import engine, db_session
from airflow.models import Variable
from include.okx.Account import AccountAPI
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


def process_item(item):
    key_mapping = {
        'instType': 'inst_type',
        'billId': 'bill_id',
        'subType': 'sub_type',
        'ts': 'ts',
        'balChg': 'bal_chg',
        'posBalChg': 'pos_bal_chg',
        'bal': 'bal',
        'posBal': 'pos_bal',
        'sz': 'sz',
        'px': 'px',
        'ccy': 'ccy',
        'pnl': 'pnl',
        'fee': 'fee',
        'mgnMode': 'mgn_mode',
        'instId': 'inst_id',
        'ordId': 'ord_id',
        'execType': 'exec_type',
        'interest': 'interest',
        'tag': 'tag',
        'fillTime': 'fill_time',
        'tradeId': 'trade_id',
        'clOrdId': 'cl_ord_id',
        'fillIdxPx': 'fill_idx_px',
        'fillMarkPx': 'fill_mark_px',
        'fillPxVol': 'fill_px_vol',
        'fillPxUsd': 'fill_px_usd',
        'fillMarkVol': 'fill_mark_vol',
        'fillFwdPx': 'fill_fwd_px'
    }
    item['ts'] = from_timestamp(item['ts'])
    item['fillTime'] = from_timestamp(item['fillTime'])
    item = process_keys(item, key_mapping)
    return item


@dag(schedule_interval='*/30 * * * *', default_args={'owner': 'Fang'}, tags=['crypto', 'sync'],
     start_date=datetime(2023, 1, 1), catchup=False)
def bills_history():
    @task
    def init_table():
        init_mysql(table=BillsHistory, engine=engine)

    @task
    def fetch_data(**kwargs):
        start_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        params = {
            'begin': int(start_time.timestamp()*1000), 
            'end':  int(end_time.timestamp()*1000)
        }

        conf = kwargs['dag_run'].conf if kwargs['dag_run'] else {}
        after = conf.get('after', None)

        if after:
            params['after'] = after
        
        logger.info(f'获取接口参数: {params}')

        api = AccountAPI(**Variable.get('okx', deserialize_json=True)).get_account_bills_archive
        data = okx_fetch(api=api, param=params)
        return data
        
    
    @task
    def process_data(raw_data):
        processed_data = [process_item(item) for item in raw_data]
        logger.info(f'数据处理完成: {len(processed_data)} items')
        return processed_data

    @task
    def sync_data(processed_data):
        write_to_mysql(processed_data, table=BillsHistory, session=db_session, type='increment')

    @task.branch
    def continue_branch(processed_data):
        branch = 'rerun_dag' if len(processed_data) > 0 else 'end_dag'
        logger.info(f'运行分支: {branch}')
        return branch

    @task
    def update_params(processed_data):
        after = processed_data[-1]['bill_id'] if processed_data else None
        params = {'after': after}
        logger.info(f'参数更新: {params}')
        return params

    
    init_table()
    raw_data = fetch_data()
    processed_data = process_data(raw_data)
    sync_data(processed_data)
    
    should_continue = continue_branch(processed_data)
    updated_params = update_params(processed_data)

    rerun_dag = TriggerDagRunOperator(
        task_id='rerun_dag',
        trigger_dag_id='{{ dag.dag_id }}',
        conf=updated_params,
        wait_for_completion=False
    )

    end_dag = PythonOperator(
        task_id='end_dag',
        python_callable=lambda: logger.info('结束运行')
    )

    should_continue >> [rerun_dag, end_dag]

bills_history()