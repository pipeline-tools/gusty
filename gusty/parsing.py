import os, yaml, frontmatter, nbformat
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


def read_yaml_spec(file):
    """
    Reading in yaml specs / frontmatter.
    """

    if file.endswith(".ipynb"):
        # Find first yaml cell in jupyter notebook and parse yaml
        nb_cells = nbformat.read(file, as_version=4)["cells"]
        yaml_cell = [
            cell
            for cell in nb_cells
            if cell["cell_type"] == "markdown"
            and cell["source"].startswith(("```yaml", "```yml"))
        ]
        assert len(yaml_cell) > 0, "Please add a yaml block to %s" % file
        yaml_cell = yaml_cell[0]["source"]
        yaml_file = yaml.safe_load(
            yaml_cell.replace("```yaml", "").replace("```yml", "").replace("```", "")
        )

    else:
        # Read either the frontmatter or the parsed yaml file (using "or" to coalesce them)
        file_parsed = frontmatter.load(file)
        yaml_file = file_parsed.metadata or yaml.load(
            file_parsed.content, Loader=GustyYAMLLoader
        )

    assert "operator" in yaml_file, "No operator specified in yaml spec " + file

    task_id = os.path.splitext(os.path.basename(file))[0]
    yaml_file["task_id"] = task_id.lower().strip()
    assert (
        yaml_file["task_id"] != "all"
    ), "Task name 'all' is not allowed. Please change your task name."

    yaml_file["file_path"] = file

    return yaml_file
