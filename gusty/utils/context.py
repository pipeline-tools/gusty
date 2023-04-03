# from airflow import macros
# from airflow.utils.context import VariableAccessor

from gusty.parsing.loaders import (
    default_constructors,
    handle_user_constructors,
)

# airflow_default_macros = {
#     # Cannot include macros because Airflow uses deepcopy
#     # "macros": macros,
#     "var": {
#         "json": VariableAccessor(deserialize_json=True),
#         "value": VariableAccessor(deserialize_json=False),
#     },
# }


def generate_loader_constructors(user_defined_macros, dag_constructors):
    loader_constructors = {}
    loader_constructors.update(default_constructors)
    loader_constructors.update(handle_user_constructors(dag_constructors))

    # user-defined macros are always preferred over
    # any default_constructors or dag_constructors,
    # and have to be formatted for YAML tags
    user_defined_constructors = {}
    for tag, func in user_defined_macros.items():
        tag = "!" + tag
        user_defined_constructors[tag] = func
    loader_constructors.update(user_defined_constructors)

    return loader_constructors


def generate_runner_context(loader_constructors):
    # Assumes that consolidation has already
    # occurred via generate_loader_constructors
    runner_context = {k.strip("!"): v for k, v in loader_constructors.items()}
    # support for airflow_default_macros, as long as the
    # user hasn't already added them via in user_defined_macros
    # or dag_constructors
    # for k, v in airflow_default_macros.items():
    #     if k not in runner_context.keys():
    #         runner_context[k] = v
    return runner_context


def generate_user_defined_macros(user_defined_macros, dag_constructors):
    macros_plus_constructors = {}
    macros_plus_constructors.update(
        {k.strip("!"): v for k, v in handle_user_constructors(dag_constructors).items()}
    )
    macros_plus_constructors.update(user_defined_macros)
    return macros_plus_constructors
