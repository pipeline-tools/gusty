from datetime import datetime, date
from typing import TypedDict, Literal


class RangeForIntervalParams(TypedDict, total=False):
    """Range для генерации тасков."""

    from_: int | datetime | date
    to_: int | datetime | date
    increment: int
    range_type: Literal['integers', 'days']


class MultiTaskGenerator(TypedDict):
    """Автогенератор тасков."""

    range_for_interval_params: RangeForIntervalParams
    interval_params: list[str]
