from dataclasses import dataclass

from gusty.models import DagErrorData


@dataclass
class GustyDagsImportError(ImportError):
    """Ошибка импорта."""

    dag_errors: list[DagErrorData]

    def __str__(self) -> str:
        """Строковое представление ошибки."""
        errors_str = ", ".join([f"{err.dag_id}: {str(err.error)}" for err in self.dag_errors])
        return f'Ошибки импорта ДАГов: {errors_str}'
