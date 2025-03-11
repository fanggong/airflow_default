from sqlalchemy import Column, String, Integer, DECIMAL, DateTime
from include.database.mysql_own import Base


class DepositHistory(Base):
    __tablename__ = 'deposit_history'

    dep_id = Column(String(255), primary_key=True, nullable=False, default='', comment='充值记录 ID')
    from_ = Column(String(255), name='from', nullable=True, default='', comment='充值账户')
    from_wd_id = Column(String(255), nullable=True, comment='内部转账发起者提币申请 ID')
    state = Column(Integer, nullable=True, comment='充值状态')
    to = Column(String(255), nullable=True, default='', comment='到账地址')
    ts = Column(DateTime, nullable=True, comment='充值记录创建时间')
    tx_id = Column(String(255), nullable=True, default='', comment='区块转账哈希记录')
    actual_dep_blk_confirm = Column(Integer, nullable=True, comment='最新的充币网络确认数')
    amt = Column(DECIMAL(38, 16), nullable=True, comment='数量')
    area_code_from = Column(String(255), nullable=True, default='', comment='手机号的区号')
    ccy = Column(String(255), nullable=True, default='', comment='币种')
    chain = Column(String(255), nullable=True, default='', comment='币种链信息')

    def __repr__(self):
        return f"<DepositHistory(dep_id='{self.dep_id}', from_account='{self.from_account}', to='{self.to}', amt={self.amt})>"
