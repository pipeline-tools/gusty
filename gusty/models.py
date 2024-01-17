from typing import NamedTuple


class DagErrorData(NamedTuple):
    """Данные ошибки DAG."""

    dag_id: str
    error: Exception
