import os
from gusty.parsing.parsers import parse_generic, parse_py, parse_ipynb, parse_sql

default_parsers = {
    ".yml": parse_generic,
    ".yaml": parse_generic,
    ".Rmd": parse_generic,
    ".py": parse_py,
    ".ipynb": parse_ipynb,
    ".sql": parse_sql,
}


def parse(file_path, parse_dict=default_parsers):
    """
    Reading in yaml specs / frontmatter.
    """

    path, extension = os.path.splitext(file_path)

    parser = parse_dict[extension]

    yaml_file = parser(file_path)

    assert "operator" in yaml_file, "No operator specified in yaml spec " + file_path

    # gusty always supplies a task_id and a file_path in a spec
    yaml_file["task_id"] = (
        os.path.splitext(os.path.basename(file_path))[0].lower().strip()
    )
    assert (
        yaml_file["task_id"] != "all"
    ), "Task name 'all' is not allowed. Please change your task name."

    yaml_file["file_path"] = file_path

    # Check dependencies
    if "dependencies" in yaml_file.keys():
        assert isinstance(
            yaml_file["dependencies"], list
        ), "dependencies needs to be a list of strings in {file_path}".format(
            file_path=file_path
        )
        assert all(
            [isinstance(dep, str) for dep in yaml_file["dependencies"]]
        ), "external_dependencies needs to be a list of strings in {file_path}".format(
            file_path=file_path
        )
    if "external_dependencies" in yaml_file.keys():
        assert isinstance(
            yaml_file["external_dependencies"], list
        ), "external_dependencies needs to be a list of dicts in {file_path}".format(
            file_path=file_path
        )
        assert all(
            [isinstance(dep, dict) for dep in yaml_file["external_dependencies"]]
        ), "external_dependencies needs to be a list of dicts in {file_path}".format(
            file_path=file_path
        )

    return yaml_file
