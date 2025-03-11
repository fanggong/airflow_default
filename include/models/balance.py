from sqlalchemy import Column, DECIMAL, String, DateTime
from include.database.mysql_own import Base


class Balance(Base):
    __tablename__ = 'balance'

    acc_avg_px = Column(DECIMAL(38, 16), nullable=True)
    avail_bal = Column(DECIMAL(38, 16), nullable=True)
    avail_eq = Column(DECIMAL(38, 16), nullable=True)
    borrow_froz = Column(DECIMAL(38, 16), nullable=True)
    cash_bal = Column(DECIMAL(38, 16), nullable=True)
    ccy = Column(String(255), nullable=True, primary_key=True)
    cl_spot_in_use_amt = Column(DECIMAL(38, 16), nullable=True)
    cross_liab = Column(DECIMAL(38, 16), nullable=True)
    dis_eq = Column(DECIMAL(38, 16), nullable=True)
    eq = Column(DECIMAL(38, 16), nullable=True)
    eq_usd = Column(DECIMAL(38, 16), nullable=True)
    fixed_bal = Column(DECIMAL(38, 16), nullable=True)
    frozen_bal = Column(DECIMAL(38, 16), nullable=True)
    imr = Column(DECIMAL(38, 16), nullable=True)
    interest = Column(DECIMAL(38, 16), nullable=True)
    iso_eq = Column(DECIMAL(38, 16), nullable=True)
    iso_liab = Column(DECIMAL(38, 16), nullable=True)
    iso_upl = Column(DECIMAL(38, 16), nullable=True)
    liab = Column(DECIMAL(38, 16), nullable=True)
    max_loan = Column(DECIMAL(38, 16), nullable=True)
    max_spot_in_use = Column(DECIMAL(38, 16), nullable=True)
    mgn_ratio = Column(DECIMAL(38, 16), nullable=True)
    mmr = Column(DECIMAL(38, 16), nullable=True)
    notional_lever = Column(DECIMAL(38, 16), nullable=True)
    open_avg_px = Column(DECIMAL(38, 16), nullable=True)
    ord_frozen = Column(DECIMAL(38, 16), nullable=True)
    reward_bal = Column(DECIMAL(38, 16), nullable=True)
    smt_sync_eq = Column(DECIMAL(38, 16), nullable=True)
    spot_bal = Column(DECIMAL(38, 16), nullable=True)
    spot_in_use_amt = Column(DECIMAL(38, 16), nullable=True)
    spot_iso_bal = Column(DECIMAL(38, 16), nullable=True)
    spot_upl = Column(DECIMAL(38, 16), nullable=True)
    spot_upl_ratio = Column(DECIMAL(38, 16), nullable=True)
    stgy_eq = Column(DECIMAL(38, 16), nullable=True)
    total_pnl = Column(DECIMAL(38, 16), nullable=True)
    total_pnl_ratio = Column(DECIMAL(38, 16), nullable=True)
    twap = Column(DECIMAL(38, 16), nullable=True)
    u_time = Column(DateTime, nullable=True)
    upl = Column(DECIMAL(38, 16), nullable=True)
    upl_liab = Column(DECIMAL(38, 16), nullable=True)

    def __repr__(self):
        return f"<Balance(ccy='{self.ccy}', u_time={self.u_time})>"
