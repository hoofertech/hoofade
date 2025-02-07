from sqlalchemy import Column, String, DateTime
from sqlalchemy.types import TypeDecorator
import datetime
from .db_trade import Base


class TZDateTime(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.timezone.utc)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.timezone.utc)
        return value


class DBPortfolio(Base):
    __tablename__ = "portfolio_posts"

    source_id = Column(String, primary_key=True)
    last_post = Column(TZDateTime, nullable=False)
