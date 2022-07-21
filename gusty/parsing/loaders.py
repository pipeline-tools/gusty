from absql.files.loader import generate_loader as generate_absql_loader
from datetime import datetime, timedelta
from gusty.utils import days_ago

default_constructors = {
    "!days_ago": days_ago,
    "!timedelta": timedelta,
    "!datetime": datetime,
}


def generate_loader(custom_constructors=None):
    """Generates a SafeLoader with both standard Airflow and custom constructors"""
    dag_yaml_tags = default_constructors.copy()
    custom_constructors = custom_constructors or {}

    if isinstance(custom_constructors, list) and len(custom_constructors) > 0:
        custom_constructors = {
            ("!" + func.__name__): func for func in custom_constructors
        }

    if len(custom_constructors) > 0:
        dag_yaml_tags.update(custom_constructors)

    return generate_absql_loader(dag_yaml_tags)


def handle_user_constructors(user_constructors):
    if isinstance(user_constructors, list):
        user_constructors = {("!" + func.__name__): func for func in user_constructors}
    return user_constructors
