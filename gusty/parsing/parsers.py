import yaml, ast, importlib.util, frontmatter, nbformat, jupytext
from gusty.parsing.loaders import GustyYAMLLoader
from gusty.importing import airflow_version

if airflow_version > 1:
    from airflow.operators.python import PythonOperator
else:
    from airflow.operators.python_operator import PythonOperator


def parse_generic(file_path):
    # Read either the frontmatter or the parsed yaml file (using "or" to coalesce them)
    file_parsed = frontmatter.load(file_path)
    yaml_file = file_parsed.metadata or yaml.load(
        file_parsed.content, Loader=GustyYAMLLoader
    )

    return yaml_file


def parse_py(file_path):
    yaml_front = {}
    if airflow_version > 1:
        yaml_front.update({"operator": "airflow.operators.python.PythonOperator"})
    else:
        yaml_front.update(
            {"operator": "airflow.operators.python_operator.PythonOperator"}
        )

    spec = jupytext.read(file_path)["cells"][0]

    # if spec contains metadata header...
    if spec["cell_type"] == "raw":
        assert (
            spec["source"] is not None
        ), "You need a comment block starting and ending with '# ---' at the top of {file_path}".format(
            file_path=file_path
        )
        assert (
            "---" in spec["source"],
        ), "You need a comment block starting and ending with '# ---' at the top of {file_path}".format(
            file_path=file_path
        )
        source = spec["source"].replace("---", "")
        settings = yaml.load(source, Loader=GustyYAMLLoader)
        yaml_front.update(**settings)

        # search for a python callable if one is specified
        if "python_callable" in yaml_front.keys():
            with open(file_path) as f:
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
                    mod_file = importlib.util.spec_from_file_location(
                        "".join(i for i in file_path if i.isalnum()), file_path
                    )
                    mod = importlib.util.module_from_spec(mod_file)
                    mod_file.loader.exec_module(mod)
                    yaml_front.update(
                        {"python_callable": getattr(mod, yaml_front["python_callable"])}
                    )
                else:
                    assert (
                        False
                    ), "{file_path} specifies python_callable {callable} but {callable} not found in {file_path}".format(
                        file_path=file_path, callable=yaml_front["python_callable"]
                    )
        # Default to sourcing this file for a PythonOperator
        else:
            yaml_front.update({"python_callable": lambda: exec(open(file_path).read())})
    # If no metadata then we also default to sourcing this file for a PythonOperator
    else:
        yaml_front.update({"python_callable": lambda: exec(open(file_path).read())})

    return yaml_front


def parse_ipynb(file_path):
    # Find first yaml cell in jupyter notebook and parse yaml
    nb_cells = nbformat.read(file_path, as_version=4)["cells"]
    yaml_cell = [
        cell
        for cell in nb_cells
        if cell["cell_type"] == "markdown"
        and cell["source"].startswith(("```yaml", "```yml"))
    ]
    assert len(yaml_cell) > 0, "Please add a yaml block to %s" % file_path
    yaml_cell = yaml_cell[0]["source"]
    yaml_file = yaml.safe_load(
        yaml_cell.replace("```yaml", "").replace("```yml", "").replace("```", "")
    )

    return yaml_file


def parse_sql(file_path):
    file_parsed = frontmatter.load(file_path)
    yaml_file = file_parsed.metadata
    yaml_file["sql"] = file_parsed.content
    return yaml_file
