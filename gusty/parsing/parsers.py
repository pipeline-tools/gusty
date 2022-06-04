import yaml, ast, importlib.util, nbformat, jupytext
from gusty.parsing.loaders import generate_loader
from gusty.importing import airflow_version
from absql.files.parsers import frontmatter_load
from gusty.parsing.utils import render_frontmatter


def parse_generic(file_path, loader=None, runner=None):
    if loader is None:
        loader = generate_loader()
    # Read either the frontmatter or the parsed yaml file (using "or" to coalesce them)
    file_contents = frontmatter_load(file_path, loader=loader)
    job_spec = file_contents["metadata"] or file_contents["content"]

    job_spec = render_frontmatter(job_spec, runner)

    return job_spec


def parse_py(file_path, loader=None, runner=None):
    if loader is None:
        loader = generate_loader()

    job_spec = {}
    if airflow_version > 1:
        job_spec.update({"operator": "airflow.operators.python.PythonOperator"})
    else:
        job_spec.update(
            {"operator": "airflow.operators.python_operator.PythonOperator"}
        )

    file_contents = jupytext.read(file_path)["cells"][0]

    # if spec contains metadata header...
    if file_contents["cell_type"] == "raw":
        assert (
            file_contents["source"] is not None
        ), "You need a comment block starting and ending with '# ---' at the top of {file_path}".format(
            file_path=file_path
        )
        assert (  # noqa
            "---" in file_contents["source"],
        ), "You need a comment block starting and ending with '# ---' at the top of {file_path}".format(
            file_path=file_path
        )
        source = file_contents["source"].replace("---", "")
        settings = yaml.load(source, Loader=loader)
        job_spec.update(**settings)

        # search for a python callable if one is specified
        if "python_callable" in job_spec.keys():
            with open(file_path) as f:
                tree = ast.parse(f.read())

                class Visitor(ast.NodeVisitor):
                    def __init__(self):
                        self.has_callable = None

                    def visit_FunctionDef(self, node):
                        ast.NodeVisitor.generic_visit(self, node)
                        if node.name == job_spec["python_callable"]:
                            self.has_callable = True

                v = Visitor()
                v.visit(tree)
                if v.has_callable:
                    mod_file = importlib.util.spec_from_file_location(
                        "".join(i for i in file_path if i.isalnum()), file_path
                    )
                    mod = importlib.util.module_from_spec(mod_file)
                    mod_file.loader.exec_module(mod)
                    job_spec.update(
                        {"python_callable": getattr(mod, job_spec["python_callable"])}
                    )
                else:
                    assert (
                        False
                    ), "{file_path} specifies python_callable {callable} but {callable} not found in {file_path}".format(  # noqa
                        file_path=file_path, callable=job_spec["python_callable"]
                    )
        # Default to sourcing this file for a PythonOperator
        else:
            job_spec.update({"python_callable": lambda: exec(open(file_path).read())})
    # If no metadata then we also default to sourcing this file for a PythonOperator
    else:
        job_spec.update({"python_callable": lambda: exec(open(file_path).read())})

    job_spec = render_frontmatter(job_spec, runner)

    return job_spec


def parse_ipynb(file_path, loader=None, runner=None):
    if loader is None:
        loader = generate_loader()
    # Find first yaml cell in jupyter notebook and parse yaml
    file_contents = nbformat.read(file_path, as_version=4)["cells"]
    yaml_cell = [
        cell
        for cell in file_contents
        if cell["cell_type"] == "markdown"
        and cell["source"].startswith(("```yaml", "```yml"))
    ]
    assert len(yaml_cell) > 0, "Please add a yaml block to %s" % file_path
    yaml_cell = yaml_cell[0]["source"]
    job_spec = yaml.load(
        yaml_cell.replace("```yaml", "").replace("```yml", "").replace("```", ""),
        Loader=loader,
    )

    job_spec = render_frontmatter(job_spec, runner)

    return job_spec


def parse_sql(file_path, loader=None, runner=None):
    if loader is None:
        loader = generate_loader()
    file_contents = frontmatter_load(file_path, loader=loader)
    job_spec = file_contents["metadata"]
    job_spec["sql"] = file_contents["content"]

    job_spec = render_frontmatter(job_spec, runner)

    return job_spec
