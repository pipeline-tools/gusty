import yaml
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


class GustyYAMLLoader(yaml.SafeLoader):
    """
    Loader used for loading DAG metadata. Has several custom
    tags that support common Airflow metadata operations, such as
    days_ago, timedelta, and datetime.
    """

    def __init__(self, *args, **kwargs):
        """Initialize the Loader"""
        super(GustyYAMLLoader, self).__init__(*args, **kwargs)
        dag_yaml_tags = {
            "!days_ago": days_ago,
            "!timedelta": timedelta,
            "!datetime": datetime,
        }

        for tag, func in dag_yaml_tags.items():
            self.add_constructor(tag, self.wrap_yaml(func))

    def wrap_yaml(self, func):
        """Turn a function into one that can be run on a YAML input"""

        def ret(loader, x):
            value = self.node_converter(x)

            if isinstance(value, list):
                return func(*value)

            if isinstance(value, dict):
                return func(**value)

            return func(value)

        return ret

    def node_converter(self, x):
        """
        Converts YAML nodes of varying types into Python values,
        lists, and dictionaries
        """
        if isinstance(x, yaml.ScalarNode):
            # "I am an atomic value"
            return yaml.load(x.value, yaml.SafeLoader)
        if isinstance(x, yaml.SequenceNode):
            # "I am a list"
            return [self.scalar_to_value(v) for v in x.value]
        if isinstance(x, yaml.MappingNode):
            # "I am a dict"
            return {
                self.scalar_to_value(v[0]): self.scalar_to_value(v[1]) for v in x.value
            }

    def scalar_to_value(self, scalar):
        """
        Converts a YAML ScalarNode to its underlying Python value
        """
        type = scalar.tag.split(":")[-1]
        val = scalar.value
        return eval("{type}('{val}')".format(type=type, val=val))
