"""NYC wall clock stored as UTC (DESIGN.md §5) and the replay-shift week mapping."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

NYC = ZoneInfo("America/New_York")
# Same constants as connect/replay-shift.yaml
ANCHOR = datetime(2022, 12, 5, tzinfo=timezone.utc)  # a Monday
CYCLE = timedelta(days=84)


def ny_now() -> datetime:
    """Current NYC wall-clock time, labelled as UTC."""
    return datetime.now(NYC).replace(tzinfo=timezone.utc)


def to_history(ts: datetime) -> datetime:
    """Map a live timestamp to the same weekday and time of day in the history window."""
    return ANCHOR + (ts - ANCHOR) % CYCLE


def ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
