from airflow.decorators import task, dag
from airflow.models import Variable

from include.utils.utils import from_timestamp, process_keys
from include.models.instruments import Instruments
from include.database.mysql_own import engine, db_session
from include.service import init_mysql, write_to_mysql, okx_fetch
from include.okx.PublicData import PublicAPI

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from datetime import datetime
import logging
import json


logger = logging.getLogger(__name__)


def process_item(item):
    key_mapping = {
        'alias': 'alias',
        'baseCcy': 'base_ccy',
        'category': 'category',
        'ctMult': 'ct_mult',
        'ctType': 'ct_type',
        'ctVal': 'ct_val',
        'ctValCcy': 'ct_val_ccy',
        'expTime': 'exp_time',
        'instFamily': 'inst_family',
        'instId': 'inst_id',
        'instType': 'inst_type',
        'lever': 'level',
        'listTime': 'list_time',
        'lotSz': 'lot_sz',
        'maxIcebergSz': 'max_iceberg_sz',
        'maxLmtAmt': 'max_lmt_amt',
        'maxLmtSz': 'max_lmt_sz',
        'maxMktAmt': 'max_mkt_amt',
        'maxMktSz': 'max_mkt_sz',
        'maxStopSz': 'max_stop_sz',
        'maxTriggerSz': 'max_trigger_sz',
        'maxTwapSz': 'max_twap_sz',
        'minSz': 'min_sz',
        'optType': 'opt_type',
        'quoteCcy': 'quote_ccy',
        'ruleType': 'rule_type',
        'settleCcy': 'settle_ccy',
        'state': 'state',
        'stk': 'stk',
        'tickSz': 'tick_sz',
        'uly': 'uly'
    }
    item['expTime'] = from_timestamp(item['expTime'])
    item['listTime'] = from_timestamp(item['listTime'])
    item = process_keys(item, key_mapping)
    return item


@dag(schedule_interval='*/30 * * * *', default_args={'owner': 'Fang'}, tags=['crypto', 'sync'],
     start_date=datetime(2023, 1, 1), catchup=False)
def instruments():
    @task
    def init_table():
        init_mysql(table=Instruments, engine=engine)
    
    @task(retries=5, retry_delay=10)
    def fetch_write_data(inst_type, **kwargs):
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(
            SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']
        ))
        api = PublicAPI().get_instruments
        raw_data = okx_fetch(api=api, param={'instType': inst_type})
        path = f'instruments/{kwargs['ds_nodash']}/{inst_type}.json'
        client.put_object(
            Bucket=cos_config['bucket'],
            Body=json.dumps(raw_data, indent=2),
            Key=path
        )

        return path

    @task
    def read_sync_data(path):
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(
            SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']
        ))
        raw_data = client.get_object(
            Bucket=cos_config['bucket'],
            Key=path
        )
        raw_data: StreamBody = raw_data['Body'].get_raw_stream().read()
        raw_data = json.loads(raw_data)
        processed_data = [process_item(item) for item in raw_data]
        write_to_mysql(data=processed_data, table=Instruments, session=db_session, type='increment')

    init_table()
    inst_type = ['SPOT', 'MARGIN', 'SWAP']
    path = fetch_write_data.expand(inst_type=inst_type)
    read_sync_data.expand(path=path)

instruments()


