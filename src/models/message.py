from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any


@dataclass
class Message:
    content: str
    timestamp: datetime
    metadata: Dict[str, Any]
