from sqlalchemy import Column, String, DECIMAL, DateTime, Integer
from include.database.mysql_own import Base


class WithdrawHistory(Base):
    __tablename__ = 'withdraw_history'

    chain = Column(String(255), nullable=True, default='', comment='币种链信息')
    area_code_from = Column(String(255), nullable=True, default='', comment='手机号区号（from）')
    client_id = Column(String(255), nullable=True, default='', comment='客户自定义ID')
    fee = Column(DECIMAL(38, 16), nullable=True, comment='提币手续费数量')
    amt = Column(DECIMAL(38, 16), nullable=True, comment='数量')
    tx_id = Column(String(255), nullable=True, default='', comment='提币哈希记录')
    area_code_to = Column(String(255), nullable=True, default='', comment='手机号区号（to）')
    ccy = Column(String(255), nullable=True, default='', comment='币种')
    from_ = Column(String(255), name='from', nullable=True, default='', comment='提币账户')
    to = Column(String(255), nullable=True, default='', comment='收币地址')
    state = Column(Integer, nullable=True, comment='提币状态')
    non_tradable_asset = Column(String(255), nullable=True, default='', comment='是否为不可交易资产')
    ts = Column(DateTime, nullable=True, comment='提币申请时间')
    wd_id = Column(String(255), primary_key=True, nullable=False, default='', comment='提币申请ID')
    fee_ccy = Column(String(255), nullable=True, default='', comment='提币手续费币种')

    def __repr__(self):
        return f"<WithdrawHistory(wd_id='{self.wd_id}', ccy='{self.ccy}', amt={self.amt})>"
