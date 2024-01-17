from dataclasses import dataclass


@dataclass
class GustyDagsImportError(ImportError):
    """Ошибка импорта."""

    dags: list[str]
    errors: list[Exception]

    def __str__(self) -> str:
        """Строковое представление ошибки."""
        errors_str = ", ".join([f"{dag_id}: {str(err)}" for dag_id, err in zip(self.dags, self.errors)])
        return f'Ошибки импорта ДАГов: {errors_str}'
