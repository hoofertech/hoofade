from dataclasses import dataclass
from datetime import datetime


@dataclass
class DBPortfolio:
    __tablename__ = "portfolio_posts"

    source_id: str
    last_post: datetime
