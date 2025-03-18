from airflow.decorators import task, dag
from airflow.models import Param
from include.service import init_mysql, write_to_mysql
from include.database.mysql_own import engine, db_session
from include.models.c2c import C2C
from datetime import datetime

# 定义参数（用于 Web UI 弹出输入框）
params = {
    'oid': Param('', type='string', description='订单编号'),
    'side': Param('buy', type='string', enum=['buy', 'sell'], description='交易方向（buy 入金, sell 出金）'),
    'amt': Param(0.0, type='number', description='数量（USDT）'),
    'value': Param(0.0, type='number', description='价值（CNY）'),
    'unit_price': Param(0.0, type='number', description='单价（价值/数量）'),
    'ts': Param(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), type='string', description='订单时间（格式: YYYY-MM-DD HH:MM:SS）'),
}

@dag(schedule=None, params=params, default_args={'owner': 'Fang'}, tags=['cyrpto', 'sync'])
def c2c():
    @task
    def init_table():
        init_mysql(table=C2C, engine=engine)

    @task
    def order_input(**kwargs):
        conf = kwargs.get('params', {})
        conf['ts'] = datetime.strptime(conf['ts'], "%Y-%m-%d %H:%M:%S")    
        return conf
    
    @task
    def sync_data(data):
        write_to_mysql(data=[data], table=C2C, session=db_session, type='increment')


    init_table()
    order_info = order_input()
    sync_data(order_info)

c2c()
