from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from include.service import init_mysql, okx_fetch, sync_mysql
from include.models.deposit_history import DepositHistory
from include.utils.utils import from_timestamp, process_keys
from include.database.mysql_own import engine, db_session
from airflow.models import Variable
from include.okx.Funding import FundingAPI
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


def process_item(item):
    key_mapping = {
        'actualDepBlkConfirm': 'actual_dep_blk_confirm',
        'amt': 'amt',
        'areaCodeFrom': 'area_code_from',
        'ccy': 'ccy',
        'chain': 'chain',
        'depId': 'dep_id',
        'from': 'from',
        'fromWdId': 'from_wd_id',
        'state': 'state',
        'to': 'to',
        'ts': 'ts',
        'txId': 'tx_id'
    }
    item['ts'] = from_timestamp(item['ts'])
    item = process_keys(item, key_mapping)
    return item


@dag(schedule_interval='*/30 * * * *', default_args={'owner': 'Fang'}, tags=['crypto', 'sync'],
     start_date=datetime(2023, 1, 1), catchup=False)
def deposit_history():
    @task
    def get_parameters(**kwargs):
        # start_time = kwargs['data_interval_start']
        start_time = datetime(2022, 1, 1)
        params = {
            'before': int(start_time.timestamp()*1000)
        }

        conf = kwargs['dag_run'].conf if kwargs['dag_run'] else {}
        before = conf.get('before', None)

        if before:
            params['before'] = before
        
        logger.info(f'获取接口参数: {params}')
        return params
    
    @task
    def process_data(raw_data):
        if raw_data['code'] == '0':
            raw_data = raw_data['data']
            processed_data = [process_item(item) for item in raw_data]
            logger.info(f'数据处理完成: {len(processed_data)} items')
            return processed_data
        else:
            return []


    @task.branch
    def continue_branch(processed_data):
        branch = 'rerun_dag' if len(processed_data) > 0 else 'end_dag'
        logger.info(f'运行分支: {branch}')
        return branch

    @task
    def update_params(raw_data):
        before = raw_data['data'][0]['ts'] if raw_data['data'] else None
        params = {'before': before}
        logger.info(f'参数更新: {params}')
        return params
    
    init_mysql(table=DepositHistory, engine=engine)

    config = Variable.get('okx', deserialize_json=True)
    api = FundingAPI(**config).get_deposit_history

    params = get_parameters()
    raw_data = okx_fetch(api=api, params=params)
    processed_data = process_data(raw_data)

    sync_mysql(processed_data, table=DepositHistory, session=db_session, type='increment')

    should_continue = continue_branch(processed_data)
    updated_params = update_params(raw_data=raw_data)

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

deposit_history()