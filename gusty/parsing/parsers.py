import yaml, nbformat, jupytext
from gusty.parsing.loaders import generate_loader
from gusty.importing import airflow_version
from absql.files.parsers import frontmatter_load
from gusty.parsing.utils import render_frontmatter, get_callable_from_file


def parse_generic(file_path, loader=None, runner=None, render_on_create=False):
    if loader is None:
        loader = generate_loader()
    # Read either the frontmatter or the parsed yaml file (using "or" to coalesce them)
    file_contents = frontmatter_load(file_path, loader=loader)
    job_spec = file_contents["metadata"] or file_contents["content"]

    if render_on_create:
        job_spec = render_frontmatter(job_spec, runner)

    return job_spec


def parse_py(file_path, loader=None, runner=None, render_on_create=False):
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
            callable_name = job_spec["python_callable"]
            job_spec.update(
                {"python_callable": get_callable_from_file(file_path, callable_name)}
            )
        # Default to sourcing this file for a PythonOperator
        else:
            job_spec.update({"python_callable": lambda: exec(open(file_path).read())})

        # Support additional callables
        if "extra_callables" in job_spec.keys():
            for arg_name, callable_name in job_spec["extra_callables"].items():
                job_spec.update(
                    {arg_name: get_callable_from_file(file_path, callable_name)}
                )

    # If no metadata then we also default to sourcing this file for a PythonOperator
    else:
        job_spec.update({"python_callable": lambda: exec(open(file_path).read())})

    if render_on_create:
        job_spec = render_frontmatter(job_spec, runner)

    return job_spec


def parse_ipynb(file_path, loader=None, runner=None, render_on_create=False):
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

    if render_on_create:
        job_spec = render_frontmatter(job_spec, runner)

    return job_spec


def parse_sql(file_path, loader=None, runner=None, render_on_create=False):
    if loader is None:
        loader = generate_loader()
    file_contents = frontmatter_load(file_path, loader=loader)
    job_spec = file_contents["metadata"]
    job_spec["sql"] = file_contents["content"]

    if render_on_create:
        job_spec = render_frontmatter(job_spec, runner)

    return job_spec
