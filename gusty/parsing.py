import os, yaml, ast, importlib.util, inspect, frontmatter, nbformat, jupytext
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from gusty.importing import airflow_version, get_operator

if airflow_version > 1:
    from airflow.operators.python import PythonOperator
else:
    from airflow.operators.python_operator import PythonOperator


def parse_py_as_module(task_id, file):
    yaml_front = {}
    if airflow_version > 1:
        yaml_front.update({"operator": "airflow.operators.python.PythonOperator"})
    else:
        yaml_front.update(
            {"operator": "airflow.operators.python_operator.PythonOperator"}
        )

    spec = jupytext.read(file)["cells"][0]

    # if spec contains metadata header...
    if spec["cell_type"] == "raw":
        assert (
            spec["source"] is not None
        ), "You need a comment block starting and ending with '# ---' at the top of {file}".format(
            file=file
        )
        assert (
            "---" in spec["source"],
        ), "You need a comment block starting and ending with '# ---' at the top of {file}".format(
            file=file
        )
        source = spec["source"].replace("---", "")
        settings = yaml.load(source, Loader=GustyYAMLLoader)
        yaml_front.update(**settings)

        # search for a python callable if one is specified
        if "python_callable" in yaml_front.keys():
            with open(file) as f:
                tree = ast.parse(f.read())

                class Visitor(ast.NodeVisitor):
                    def __init__(self):
                        self.has_callable = None

                    def visit_FunctionDef(self, node):
                        ast.NodeVisitor.generic_visit(self, node)
                        if node.name == yaml_front["python_callable"]:
                            self.has_callable = True

                v = Visitor()
                v.visit(tree)
                if v.has_callable:
                    mod_file = importlib.util.spec_from_file_location(task_id, file)
                    mod = importlib.util.module_from_spec(mod_file)
                    mod_file.loader.exec_module(mod)
                    yaml_front.update(
                        {"python_callable": getattr(mod, yaml_front["python_callable"])}
                    )
                else:
                    assert (
                        False
                    ), "{file} specifies python_callable {callable} but {callable} not found in {file}".format(
                        file=file, callable=yaml_front["python_callable"]
                    )
        # Default to sourcing this file for a PythonOperator
        else:
            yaml_front.update({"python_callable": lambda: exec(open(file).read())})
    # If no metadata then we also default to sourcing this file for a PythonOperator
    else:
        yaml_front.update({"python_callable": lambda: exec(open(file).read())})

    return yaml_front


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

    task_id = os.path.splitext(os.path.basename(file))[0]

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

    elif file.endswith(".py"):
        yaml_file = parse_py_as_module(task_id, file)

    else:
        # Read either the frontmatter or the parsed yaml file (using "or" to coalesce them)
        file_parsed = frontmatter.load(file)
        yaml_file = file_parsed.metadata or yaml.load(
            file_parsed.content, Loader=GustyYAMLLoader
        )

    assert "operator" in yaml_file, "No operator specified in yaml spec " + file

    yaml_file["task_id"] = task_id.lower().strip()
    assert (
        yaml_file["task_id"] != "all"
    ), "Task name 'all' is not allowed. Please change your task name."

    yaml_file["file_path"] = file

    # Check dependencies
    if "dependencies" in yaml_file.keys():
        assert isinstance(
            yaml_file["dependencies"], list
        ), "dependencies needs to be a list of strings in {file}".format(file=file)
        assert all(
            [isinstance(dep, str) for dep in yaml_file["dependencies"]]
        ), "external_dependencies needs to be a list of strings in {file}".format(
            file=file
        )
    if "external_dependencies" in yaml_file.keys():
        assert isinstance(
            yaml_file["external_dependencies"], list
        ), "external_dependencies needs to be a list of dicts in {file}".format(
            file=file
        )
        assert all(
            [isinstance(dep, dict) for dep in yaml_file["external_dependencies"]]
        ), "external_dependencies needs to be a list of dicts in {file}".format(
            file=file
        )

    return yaml_file
