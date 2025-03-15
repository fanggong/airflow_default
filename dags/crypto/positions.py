from airflow.decorators import task, dag
from include.service import init_mysql, okx_fetch, sync_mysql
from include.models.positions import Positions
from include.database.mysql_own import engine, db_session
from airflow.models import Variable
from include.okx.Account import AccountAPI
from include.utils.utils import from_timestamp, process_keys
from datetime import datetime


def process_item(item):
    key_mapping = {
        'adl': 'adl',
        'availPos': 'avail_pos',
        'avgPx': 'avg_px',
        'baseBal': 'base_bal',
        'baseBorrowed': 'base_borrowed',
        'baseInterest': 'base_interest',
        'bePx': 'be_px',
        'bizRefId': 'biz_ref_id',
        'bizRefType': 'biz_ref_type',
        'cTime': 'c_time',
        'ccy': 'ccy',
        'clSpotInUseAmt': 'cl_spot_in_use_amt',
        'closeOrderAlgo': 'close_order_algo',
        'deltaBS': 'delta_bs',
        'deltaPA': 'delta_pa',
        'fee': 'fee',
        'fundingFee': 'funding_fee',
        'gammaBS': 'gamma_bs',
        'gammaPA': 'gamma_pa',
        'idxPx': 'idx_px',
        'imr': 'imr',
        'instId': 'inst_id',
        'instType': 'inst_type',
        'interest': 'interest',
        'last': 'last',
        'lever': 'lever',
        'liab': 'liab',
        'liabCcy': 'liab_ccy',
        'liqPenalty': 'liq_penalty',
        'liqPx': 'liq_px',
        'margin': 'margin',
        'markPx': 'mark_px',
        'maxSpotInUseAmt': 'max_spot_in_use_amt',
        'mgnMode': 'mgn_mode',
        'mgnRatio': 'mgn_ratio',
        'mmr': 'mmr',
        'notionalUsd': 'notional_usd',
        'optVal': 'opt_val',
        'pendingCloseOrdLiabVal': 'pending_close_ord_liab_val',
        'pnl': 'pnl',
        'pos': 'pos',
        'posCcy': 'pos_ccy',
        'posId': 'pos_id',
        'posSide': 'pos_side',
        'quoteBal': 'quote_bal',
        'quoteBorrowed': 'quote_borrowed',
        'quoteInterest': 'quote_interest',
        'realizedPnl': 'realized_pnl',
        'spotInUseAmt': 'spot_in_use_amt',
        'spotInUseCcy': 'spot_in_use_ccy',
        'thetaBS': 'theta_bs',
        'thetaPA': 'theta_pa',
        'tradeId': 'trade_id',
        'uTime': 'u_time',
        'upl': 'upl',
        'uplLastPx': 'upl_last_px',
        'uplRatio': 'upl_ratio',
        'uplRatioLastPx': 'upl_ratio_last_px',
        'usdPx': 'usd_px',
        'vegaBS': 'vega_bs',
        'vegaPA': 'vega_pa'
    }
    item['closeOrderAlgo'] = None
    item['uTime'] = from_timestamp(item['uTime'])
    item['cTime'] = from_timestamp(item['cTime'])
    item = process_keys(item, key_mapping)
    return item

@dag(schedule_interval='*/30 * * * *', default_args={'owner': 'Fang'}, tags=['crypto', 'sync'],
     start_date=datetime(2023, 1, 1), catchup=False)
def positions():

    @task
    def process(data):
        if data['code'] == '0':
            data = data['data']
            data = [process_item(item) for item in data]
            return data
        else:
            return []

    config = Variable.get('okx', deserialize_json=True)
    api = AccountAPI(**config).get_positions
    init_mysql(table=Positions, engine=engine)
    data = okx_fetch(api=api)
    processed_data = process(data)
    sync_mysql(data_list=processed_data, table=Positions, session=db_session, type='full')

positions()