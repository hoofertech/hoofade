import logging
from datetime import date, datetime
from typing import Optional

from config import default_timezone

logger = logging.getLogger(__name__)


def format_datetime(dt: Optional[datetime]) -> Optional[str]:
    """
    Format datetime to string consistently across the application.

    Args:
        dt: Datetime object to format
        sep: Separator between date and time (default space)

    Returns:
        Formatted datetime string or None if input is None
    """
    if dt is None:
        return None

    # Ensure datetime is timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_timezone())

    return dt.strftime("%Y%m%dT%H%M%S.000Z")


def format_date(dt: Optional[date]) -> Optional[str]:
    """
    Format datetime to string consistently across the application.

    Args:
        dt: Datetime object to format
        sep: Separator between date and time (default space)

    Returns:
        Formatted datetime string or None if input is None
    """
    if dt is None:
        return None

    return dt.strftime("%Y-%m-%d")


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse datetime string in various formats"""
    if not dt_str:
        return None

    try:
        return datetime.fromisoformat(dt_str)
    except ValueError as e:
        logger.error(f"Failed to parse datetime: {dt_str}")
        raise ValueError(f"Unable to parse datetime string: {dt_str}") from e
