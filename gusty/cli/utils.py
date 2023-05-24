import os


def get_dags_directory(dags_dir):
    current_directory_name = os.path.split(os.getcwd())[-1]

    # Check if current, parent, or child directory is the dags directory
    if current_directory_name == "dags":
        dags_dir = "."
    elif os.path.exists("dags"):
        dags_dir = "dags"
    elif os.path.exists("../dags"):
        dags_dir = ".."
    else:
        dags_dir = "."
    return dags_dir
