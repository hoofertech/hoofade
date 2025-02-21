import json
from dataclasses import dataclass
from datetime import datetime

from utils.datetime_utils import format_datetime, parse_datetime


@dataclass
class DBMessage:
    __tablename__ = "messages"

    id: str
    content: str
    timestamp: datetime
    message_metadata: str
    source_id: str
    message_type: str  # 'trd' or 'pfl'

    def to_dict(self):
        return {
            "id": self.id,
            "content": self.content,
            "timestamp": format_datetime(self.timestamp),
            "message_metadata": json.dumps(self.message_metadata),
            "source_id": self.source_id,
            "message_type": self.message_type,
        }

    @staticmethod
    def from_dict(data: dict) -> "DBMessage":
        return DBMessage(
            id=data["id"],
            content=data["content"],
            timestamp=parse_datetime(data["timestamp"]),
            message_metadata=json.loads(data["message_metadata"]),
            source_id=data["source_id"],
            message_type=data["message_type"],
        )
