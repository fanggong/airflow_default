from sqlalchemy import Column, String, DECIMAL, DateTime
from include.database.mysql_own import Base


class C2C(Base):
    __tablename__ = 'c2c'

    oid = Column(String(255), primary_key=True, nullable=False, default='', comment='订单编号')
    side = Column(String(255), nullable=True, default='', comment='方向，buy入金，sell出金')
    amt = Column(DECIMAL(38, 16), nullable=True, comment='数量，usdt')
    value = Column(DECIMAL(38, 16), nullable=True, comment='价值，cny')
    unit_price = Column(DECIMAL(38, 16), nullable=True, comment='单价，价值/数量')
    ts = Column(DateTime, nullable=True, comment='订单时间')

    def __repr__(self):
        return f"<C2C(oid='{self.oid}', side='{self.side}', amt={self.amt}, value={self.value}, ts={self.ts})>"
