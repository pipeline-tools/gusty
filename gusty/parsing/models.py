from typing import TypedDict, Literal


class RangeForIntervalParams(TypedDict, total=False):
    from_: int
    to_: int
    increment: int
    range_type: Literal['integers']


class MultiTaskGenerator(TypedDict):
    range_for_interval_params: RangeForIntervalParams
    interval_params: tuple[str, str]
