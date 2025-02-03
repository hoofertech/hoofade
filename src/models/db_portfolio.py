from sqlalchemy import Column, String, DateTime
from .db_trade import Base


class DBPortfolio(Base):
    __tablename__ = "portfolio_posts"

    source_id = Column(String, primary_key=True)
    last_post = Column(DateTime, nullable=False)
