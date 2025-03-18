from sqlalchemy import Column, String, DECIMAL, DateTime
from include.database.mysql_own import Base


class MarkPrice(Base):
    __tablename__ = 'mark_price'

    inst_type = Column(String(255), primary_key=True, nullable=False, default='', comment='产品类型')
    inst_id = Column(String(255), primary_key=True, nullable=False, default='', comment='产品 ID')
    mark_px = Column(DECIMAL(38, 16), nullable=True, comment='标记价格')
    ts = Column(DateTime, nullable=True, comment='时间戳')

    def __repr__(self):
        return f"<MarkPrice(inst_id='{self.inst_id}', inst_type='{self.inst_type}', mark_px={self.mark_px}, ts={self.ts})>"
