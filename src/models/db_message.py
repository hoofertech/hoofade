import json

from sqlalchemy import JSON, Column, DateTime, String

from .db_trade import Base


class DBMessage(Base):
    __tablename__ = "messages"

    id = Column(String, primary_key=True)
    content = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    message_metadata = Column(JSON, nullable=False)
    source_id = Column(String, nullable=False)
    message_type = Column(String, nullable=False)  # 'trade' or 'portfolio'

    def to_dict(self):
        return {
            "id": self.id,
            "content": self.content,
            "timestamp": self.timestamp,
            "message_metadata": json.dumps(self.message_metadata),
            "source_id": self.source_id,
            "message_type": self.message_type,
        }
