import os, sys, pkgutil, itertools, airflow, importlib
from inflection import underscore

############
## Params ##
############

airflow_version = int(str(airflow.__version__)[0])

###########################
## Operator Import Logic ##
###########################


def get_operator_location(operator_string):
    """
    Get package name / determine if "local" keyword is used
    """
    # location will generally be 'airflow',
    # but if it's 'local', then we will look locally for the operator
    return operator_string.split(".")[0]


def get_operator_name(operator_string):
    """
    Get operator class
    """
    # the actual name of the operator
    return operator_string.split(".")[-1]


def get_operator_module(operator_string):
    """
    Get module name
    """
    # the module, for when the operator is not a local operator
    operator_path = ".".join(operator_string.split(".")[:-1])
    assert len(operator_path) != 0, (
        "Please specify a format like 'package.operator' to specify your operator. You passed in '%s'"
        % operator_string
    )
    return operator_path


# Add $AIRFLOW_HOME/operators directory to path for local.operator syntax to work

gusty_home = os.environ.get("GUSTY_HOME", "")
if gusty_home == "":
    CUSTOM_OPERATORS_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", ""), "operators")
else:
    CUSTOM_OPERATORS_DIR = os.path.join(gusty_home, "operators")

sys.path.append(CUSTOM_OPERATORS_DIR)
module_paths = [("", [CUSTOM_OPERATORS_DIR])]
pairs = [
    [(_.name, m + _.name) for _ in pkgutil.iter_modules(path)]
    for m, path in module_paths
]
module_dict = dict(itertools.chain(*pairs))


def get_operator(operator_string):
    """
    Given an operator string, determine the location of that operator and return the operator object
    """
    operator_name = get_operator_name(operator_string)
    operator_location = get_operator_location(operator_string)

    module_name = (
        module_dict[underscore(operator_name)]
        if operator_location == "local"
        else get_operator_module(operator_string)
    )

    operator = getattr(importlib.import_module(module_name), operator_name)
    return operator
