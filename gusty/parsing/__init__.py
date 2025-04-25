import os
from copy import copy
from datetime import datetime
from functools import partial
from typing import Any, Callable

from absql.utils import get_function_arg_names

from gusty.parsing.loaders import generate_loader
from gusty.parsing.models import RangeForIntervalParams
from gusty.parsing.parsers import parse_generic, parse_py, parse_ipynb, parse_sql
from gusty.parsing.utils import get_dates_range, set_nested_value
from gusty.utils import nested_update

T_SpecGetter = Callable[[Any, Any, list[str], dict[str, Any]], list[dict[str, Any]]]

default_parsers = {
    ".yml": parse_generic,
    ".yaml": parse_generic,
    ".Rmd": parse_generic,
    ".py": parse_py,
    ".ipynb": parse_ipynb,
    ".sql": parse_sql,
}


def parse(
    file_path,
    parse_dict=default_parsers,
    loader=None,
    runner=None,
    render_on_create=False,
):
    """
    Reading in yaml specs / frontmatter.
    """

    if loader is None:
        loader = generate_loader()

    path, extension = os.path.splitext(file_path)

    parser = parse_dict[extension]
    if "loader" in get_function_arg_names(parser):
        parser = partial(parser, loader=loader)
    if "runner" in get_function_arg_names(parser):
        parser = partial(parser, runner=runner)
    if "render_on_create" in get_function_arg_names(parser):
        parser = partial(parser, render_on_create=render_on_create)

    yaml_file = parser(file_path)

    assert "operator" in yaml_file, "No operator specified in yaml spec " + file_path

    # gusty always supplies a task_id and a file_path in a spec
    yaml_file["task_id"] = (
        os.path.splitext(os.path.basename(file_path))[0].lower().strip()
    )
    assert (
        yaml_file["task_id"] != "all"
    ), "Task name 'all' is not allowed. Please change your task name."

    yaml_file["file_path"] = file_path

    # gusty will also attach the absql_runner
    yaml_file["absql_runner"] = copy(runner)

    # Check dependencies
    if "dependencies" in yaml_file.keys():
        assert isinstance(
            yaml_file["dependencies"], list
        ), "dependencies needs to be a list of strings in {file_path}".format(
            file_path=file_path
        )
        assert all(
            [isinstance(dep, str) for dep in yaml_file["dependencies"]]
        ), "dependencies needs to be a list of strings in {file_path}".format(
            file_path=file_path
        )

    # Handle multi_task
    multi_task_spec = yaml_file.get("multi_task_spec") or {}

    python_callable = yaml_file.get("python_callable")
    python_callable_partials = yaml_file.get("python_callable_partials")
    if python_callable and python_callable_partials:
        for task_id, partial_kwargs in python_callable_partials.items():
            callable_update = {
                "python_callable": partial(python_callable, **partial_kwargs)
            }
            if task_id in multi_task_spec.keys():
                multi_task_spec[task_id].update(callable_update)
            else:
                multi_task_spec.update({task_id: callable_update})

    multi_specs = []
    if len(multi_task_spec) > 0:
        for task_id, spec in multi_task_spec.items():
            spec_for_new_task = yaml_file.copy()
            spec_for_new_task["task_id"] = task_id
            spec_for_new_task = nested_update(spec_for_new_task, spec)
            multi_specs.append(spec_for_new_task)

    multi_task_generator = yaml_file.get("multi_task_generator") or {}
    if multi_task_generator:
        _range: RangeForIntervalParams = multi_task_generator["range_for_interval_params"]
        specs_getter: T_SpecGetter = {
            'days': _get_spec_from_date_params,
            'integers': partial(
                _get_spec_from_integer_params, increment=_range.get('increment', 1),
            ),
        }[_range.get('range_type', 'integers')]
        multi_specs.extend(
            specs_getter(
                _range['from_'],
                _range['to_'],
                multi_task_generator['interval_params'],
                yaml_file.copy(),
            ),
        )

    if len(multi_specs) > 0:
        return multi_specs

    return yaml_file


def _get_spec_from_integer_params(
    start: int, end: int, param_names: list[str], spec: dict[Any, Any], increment: int,
) -> list[dict[str, Any]]:
    new_specs = []
    old_task_id = spec['task_id']
    for start_param in range(start, end, increment):
        current_spec = spec.copy()
        end_param = start_param + increment - 1
        current_spec['task_id'] = f'{old_task_id}_{start_param}_{end_param}'
        set_nested_value(current_spec, param_names[0], start_param)
        set_nested_value(current_spec, param_names[1], end_param)
        new_specs.append(current_spec)
    return new_specs


def _get_spec_from_date_params(
    start: str, end: str, param_names: list[str], spec: dict[Any, Any],
) -> list[dict[str, Any]]:
    new_specs = []
    old_task_id = spec['task_id']
    range_params = get_dates_range(datetime.fromisoformat(start), datetime.fromisoformat(end))
    for day_date in range_params:
        current_spec = spec.copy()
        set_nested_value(current_spec, param_names[0], day_date)
        if len(param_names) > 1:
            set_nested_value(current_spec, param_names[1], day_date)
        current_spec['task_id'] = f'{old_task_id}_{day_date}'
        new_specs.append(current_spec)
    return new_specs
