import yaml
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


class GustyYAMLLoader(yaml.UnsafeLoader):
    """
    Loader used for loading DAG metadata. Has several custom
    tags that support common Airflow metadata operations, such as
    days_ago, timedelta, and datetime.

    Note that this is an UnsafeLoader, so it can execute arbitrary code.
    Never run it on an untrusted YAML file (it is intended to be run
    on YAML files representing Gusty operators or metadata).
    """

    def __init__(self, *args, **kwargs):
        """Initialize the UnsafeLoader"""
        super(GustyYAMLLoader, self).__init__(*args, **kwargs)
        dag_yaml_tags = {
            "!days_ago": days_ago,
            "!timedelta": timedelta,
            "!datetime": datetime,
        }

        for tag, func in dag_yaml_tags.items():
            self.add_constructor(tag, self.wrap_yaml(func))

        # Note that you could still apply any function with e.g.
        # !!python/object/apply:datetime.timedelta [1]

    def wrap_yaml(self, func):
        """Turn a function into one that can be run on a YAML input"""

        def ret(loader, x):
            value = yaml.load(x.value, yaml.UnsafeLoader)

            if isinstance(value, list):
                return func(*value)

            if isinstance(value, dict):
                return func(**value)

            return func(value)

        return ret
