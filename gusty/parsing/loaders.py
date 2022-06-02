import yaml
from absql.files.loader import scalar_to_value, node_converter, wrap_yaml
from absql.files.loader import generate_loader as generate_absql_loader
from datetime import datetime, timedelta
from gusty.utils import days_ago


def generate_loader(custom_constructors={}):
    """Generates a SafeLoader with both standard Airflow and custom constructors"""
    dag_yaml_tags = {
        "!days_ago": days_ago,
        "!timedelta": timedelta,
        "!datetime": datetime,
    }

    if isinstance(custom_constructors, list) and len(custom_constructors) > 0:
        custom_constructors = {
            ("!" + func.__name__): func for func in custom_constructors
        }

    if len(custom_constructors) > 0:
        dag_yaml_tags.update(custom_constructors)

    return generate_absql_loader(dag_yaml_tags)
