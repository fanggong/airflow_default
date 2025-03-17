from sqlalchemy import Column, String, DECIMAL, DateTime, Integer
from include.database.mysql_own import Base


class Instruments(Base):
    __tablename__ = 'instruments'

    alias = Column(String(255), nullable=True, default='', comment='合约日期别名')
    base_ccy = Column(String(255), nullable=True, default='', comment='交易货币币种')
    category = Column(Integer, nullable=True, comment='币种类别（已废弃）')
    ct_mult = Column(Integer, nullable=True, comment='合约乘数')
    ct_type = Column(String(255), nullable=True, default='', comment='合约类型')
    ct_val = Column(DECIMAL(38, 16), nullable=True, comment='合约面值')
    ct_val_ccy = Column(String(255), nullable=True, default='', comment='合约面值计价币种')
    exp_time = Column(DateTime, nullable=True, comment='产品下线时间')
    inst_family = Column(String(255), nullable=True, default='', comment='交易品种')
    inst_id = Column(String(255), primary_key=True, nullable=False, default='', comment='产品id')
    inst_type = Column(String(255), primary_key=True, nullable=False, default='', comment='产品类型')
    level = Column(Integer, nullable=True, comment='支持的最大杠杆倍数')
    list_time = Column(DateTime, nullable=True, comment='上线时间')
    lot_sz = Column(DECIMAL(38, 16), nullable=True, comment='下单数量精度')
    max_iceberg_sz = Column(DECIMAL(38, 16), nullable=True, comment='冰山委托的单笔最大委托数量')
    max_lmt_amt = Column(DECIMAL(38, 16), nullable=True, comment='限价单的单笔最大美元价值')
    max_lmt_sz = Column(DECIMAL(38, 16), nullable=True, comment='限价单的单笔最大委托数量')
    max_mkt_amt = Column(DECIMAL(38, 16), nullable=True, comment='市价单的单笔最大美元价值')
    max_mkt_sz = Column(DECIMAL(38, 16), nullable=True, comment='市价单的单笔最大委托数量')
    max_stop_sz = Column(DECIMAL(38, 16), nullable=True, comment='止盈止损市价委托的单笔最大委托数量')
    max_trigger_sz = Column(DECIMAL(38, 16), nullable=True, comment='计划委托的单笔最大委托数量')
    max_twap_sz = Column(DECIMAL(38, 16), nullable=True, comment='时间加权单的单笔最大委托数量')
    min_sz = Column(DECIMAL(38, 16), nullable=True, comment='最小下单数量')
    opt_type = Column(String(255), nullable=True, default='', comment='期权类型')
    quote_ccy = Column(String(255), nullable=True, default='', comment='计价货币币种')
    rule_type = Column(String(255), nullable=True, default='')
    settle_ccy = Column(String(255), nullable=True, default='', comment='盈亏结算和保证金币种')
    state = Column(String(255), nullable=True, default='', comment='产品状态')
    stk = Column(DECIMAL(38, 16), nullable=True, comment='行权价格')
    tick_sz = Column(DECIMAL(38, 16), nullable=True, comment='下单价格精度')
    uly = Column(String(255), nullable=True, default='', comment='标的指数')

    def __repr__(self):
        return f"<Instruments(inst_id='{self.inst_id}', inst_type='{self.inst_type}')>"
