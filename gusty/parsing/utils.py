import ast
import importlib.util
from datetime import date, datetime, timedelta
from typing import Iterator, TypeVar, Any

from absql.render import render_context
from dateutil.relativedelta import relativedelta


def render_frontmatter(frontmatter, runner=None, exclude=["sql"]):
    if runner is not None:
        renderable_frontmatter = {
            k: v for k, v in frontmatter.items() if k not in exclude
        }
        rendered_frontmatter = render_context(
            file_contents=renderable_frontmatter, extra_context=runner.extra_context
        )
        frontmatter.update(rendered_frontmatter)
    return frontmatter


def get_callable_from_file(file_path, callable_name):
    v = CallableFinder(callable_name)
    with open(file_path) as f:
        tree = ast.parse(f.read())
        v.visit(tree)
        if v.has_callable:
            mod_file = importlib.util.spec_from_file_location(
                "".join(i for i in file_path if i.isalnum()), file_path
            )
            mod = importlib.util.module_from_spec(mod_file)
            mod_file.loader.exec_module(mod)
            return getattr(mod, callable_name)
        else:
            assert (
                False
            ), "{file_path} specifies python_callable {callable} but {callable} not found in {file_path}".format(  # noqa
                file_path=file_path, callable=callable_name
            )


class CallableFinder(ast.NodeVisitor):
    def __init__(self, callable_name):
        self.has_callable = None
        self.callable_name = callable_name

    def visit_FunctionDef(self, node):
        ast.NodeVisitor.generic_visit(self, node)
        if node.name == self.callable_name:
            self.has_callable = True


DateOrDt = TypeVar('DateOrDt', datetime, date)


def get_dates_range(date_from: DateOrDt, date_to: DateOrDt) -> Iterator[DateOrDt]:
    """Возвращает генератор дат между `date_from` и `date_to`."""
    for diff in range((date_to - date_from).days + 1):
        yield date_from + timedelta(days=diff)


def get_nested_value(dictionary: dict[str, Any], keys_path: list[str]) -> Any:
    """Возвращает значение вложенного словаря по ключу `keys_path`."""
    nested_value = dictionary.copy()
    for key in keys_path:
        nested_value = nested_value[key]
    return nested_value


def set_nested_value(
    dictionary: dict[str, Any], keys_path: list[str], value: Any,
) -> None:
    """Устанавливает значение вложенного словаря по ключу `keys_path`."""
    for key in keys_path[:-1]:
        dictionary = dictionary.setdefault(key, {})
    dictionary[keys_path[-1]] = value
