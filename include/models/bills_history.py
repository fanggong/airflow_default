from sqlalchemy import Column, String, Integer, DECIMAL, DateTime
from include.database.mysql_own import Base


class BillsHistory(Base):
    __tablename__ = 'bills_history'

    inst_type = Column(String(255), nullable=False, default='', comment='产品类型')
    bill_id = Column(String(255), primary_key=True, nullable=False, default='', comment='账单ID')
    sub_type = Column(Integer, nullable=True, comment='账单子类型')
    ts = Column(DateTime, nullable=True, comment='余额更新完成的时间，Unix时间戳的毫秒数格式，如 1597026383085')
    bal_chg = Column(DECIMAL(38, 16), nullable=True, comment='账户层面的余额变动数量')
    pos_bal_chg = Column(DECIMAL(38, 16), nullable=True, comment='仓位层面的余额变动数量')
    bal = Column(DECIMAL(38, 16), nullable=True, comment='账户层面的余额数量')
    pos_bal = Column(DECIMAL(38, 16), nullable=True, comment='仓位层面的余额数量')
    sz = Column(DECIMAL(38, 16), nullable=True, comment='数量')
    px = Column(DECIMAL(38, 16), nullable=True, comment='价格，与 subType 相关')
    ccy = Column(String(255), nullable=False, default='', comment='账户余额币种')
    pnl = Column(DECIMAL(38, 16), nullable=True, comment='收益')
    fee = Column(DECIMAL(38, 16), nullable=True, comment='手续费 正数代表平台返佣 ，负数代表平台扣除')
    mgn_mode = Column(String(255), nullable=False, default='', comment='保证金模式 isolated：逐仓 cross：全仓 无仓位类型字段，该字段返回 ""')
    inst_id = Column(String(255), nullable=False, default='', comment='产品ID，如 BTC-USDT')
    ord_id = Column(String(255), nullable=False, default='', comment='订单ID 无订单时，该字段返回 ""')
    exec_type = Column(String(255), nullable=False, default='', comment='流动性方向 T：taker M：maker')
    interest = Column(DECIMAL(38, 16), nullable=True, comment='利息')
    tag = Column(String(255), nullable=False, default='', comment='订单标签')
    fill_time = Column(DateTime, nullable=True, comment='最新成交时间')
    trade_id = Column(String(255), nullable=False, default='', comment='最新成交ID')
    cl_ord_id = Column(String(255), nullable=False, default='', comment='客户自定义订单ID')
    fill_idx_px = Column(DECIMAL(38, 16), nullable=True, comment='交易执行时的指数价格 对于交叉现货币对，返回 baseCcy-USDT 的指数价格。 例 LTC-ETH，该字段返回 LTC-USDT 的指数价格。')
    fill_mark_px = Column(DECIMAL(38, 16), nullable=True, comment='成交时的标记价格，仅适用于 交割/永续/期权')
    fill_px_vol = Column(DECIMAL(38, 16), nullable=True, comment='成交时的隐含波动率，仅适用于 期权，其他业务线返回空字符串""')
    fill_px_usd = Column(DECIMAL(38, 16), nullable=True, comment='成交时的期权价格，以USD为单位，仅适用于 期权，其他业务线返回空字符串""')
    fill_mark_vol = Column(DECIMAL(38, 16), nullable=True, comment='成交时的标记波动率，仅适用于 期权，其他业务线返回空字符串""')
    fill_fwd_px = Column(DECIMAL(38, 16), nullable=True, comment='成交时的远期价格，仅适用于 期权，其他业务线返回空字符串""')

    def __repr__(self):
        return f"<BillsHistory(bill_id='{self.bill_id}', inst_type='{self.inst_type}', ts={self.ts})>"
