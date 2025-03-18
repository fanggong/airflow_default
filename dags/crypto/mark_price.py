from airflow.decorators import task, dag
from airflow.models import Variable

from include.utils.utils import from_timestamp, process_keys
from include.models.mark_price import MarkPrice
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
        'instType': 'inst_type',
        'instId': 'inst_id',
        'markPx': 'mark_px',
        'ts': 'ts'
    }
    item['ts'] = from_timestamp(item['ts'])
    item = process_keys(item, key_mapping)
    return item


@dag(schedule_interval='*/30 * * * *', default_args={'owner': 'Fang'}, tags=['crypto', 'sync'],
     start_date=datetime(2023, 1, 1), catchup=False)
def mark_price():
    @task
    def init_table():
        init_mysql(table=MarkPrice, engine=engine)
    
    @task(retries=5, retry_delay=10)
    def fetch_write_data(inst_type, **kwargs):
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(
            SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']
        ))
        api = PublicAPI().get_mark_price
        raw_data = okx_fetch(api=api, param={'instType': inst_type})
        path = f'mark_price/{kwargs['ds_nodash']}/{inst_type}.json'
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
        raw_data = raw_data['Body'].get_raw_stream().read()
        raw_data = json.loads(raw_data)
        processed_data = [process_item(item) for item in raw_data]
        write_to_mysql(data=processed_data, table=MarkPrice, session=db_session, type='increment')

    init_table()
    inst_type = ['MARGIN', 'SWAP']
    path = fetch_write_data.expand(inst_type=inst_type)
    read_sync_data.expand(path=path)

mark_price()


