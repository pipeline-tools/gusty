import sys
import re
import os
import yaml
import pkgutil
import itertools

import airflow
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import frontmatter
import nbformat
from inflection import underscore

from .utils import GustyYAMLLoader

##########

# Hack: add AIRFLOW_HOME/operators to the Python path, for custom operators
CUSTOM_OPERATORS_DIR = os.path.join(
        os.environ.get("AIRFLOW_HOME", ""),
        "operators"
        )

sys.path.append(CUSTOM_OPERATORS_DIR)

gusty_path = [os.path.join(os.path.split(__file__)[0], "operators")]

module_paths = [
    ("airflow.operators.", airflow.operators.__path__),
    ("airflow.contrib.operators.", airflow.contrib.operators.__path__),
    ("gusty.operators.", gusty_path),
    ("", [CUSTOM_OPERATORS_DIR])
]

pairs = [[(_.name, m + _.name) for _ in pkgutil.iter_modules(path)]
    for m, path in module_paths]

module_dict = dict(itertools.chain(*pairs))

def __get_operator(operator_name):
    """Given the name as camel case"""
    if not re.match("^[A-Za-z\d]+$", operator_name):
        raise ValueError("Operator name must be an alphanumeric string")

    module_name = module_dict[underscore(operator_name)]
    import_stmt = "from %s import %s" % (module_name, operator_name)

    exec(import_stmt)
    return eval(operator_name)

###############################################
################# File System #################
###############################################

################
## Read Files ##
################

valid_extensions = ('.yml', '.Rmd', '.ipynb')

def get_files(yaml_dir):
    """
    List all file paths in a dag subdirectory
    """
    files = [os.path.join(yaml_dir, file) for file in os.listdir(yaml_dir)
        if file.endswith(valid_extensions) and file != "METADATA.yml"]
    assert len(files) > 0, ("No files with valid extensions found in %s. Valid extensions are %s" % (yaml_dir, "/".join(valid_extensions)))
    return files

########################
## Reading in yaml specs
########################

def read_yaml_spec(file):
    """
    Reading in yaml specs
    """

    if file.endswith('.ipynb'):
        # Find first yaml cell in jupyter notebook and parse yaml
        nb_cells = nbformat.read(file, as_version=4)['cells']
        yaml_cell = [cell for cell in nb_cells if cell['cell_type'] == 'markdown' and cell['source'].startswith('```yaml')][0]['source']
        yaml_file = yaml.safe_load(yaml_cell.replace('```yaml', "").replace("```", ""))

    else:
        # Read either the frontmatter or the parsed yaml file (using "or" to coalesce them)
        file_parsed = frontmatter.load(file)
        yaml_file = file_parsed.metadata or yaml.load(file_parsed.content, Loader = GustyYAMLLoader)

    assert "operator" in yaml_file, "No operator specified in yaml spec " + file

    task_id = os.path.splitext(os.path.basename(file))[0]
    yaml_file["task_id"] = task_id.lower().strip()
    assert yaml_file["task_id"] != "all", "Task name 'all' is not allowed. Please change your task name."

    yaml_file["file_path"] = file

    return yaml_file

def get_yaml_specs(directory, **kwargs):
    yaml_files = get_files(directory)
    assert len(yaml_files) > 0, "No .yml files found."
    specs = list(map(read_yaml_spec, yaml_files))
    return specs

################################################
################# Dependencies #################
################################################

#####################################
## Different kinds of dependencies ##
#####################################

# yaml spec dependencies

def get_yaml_spec_dependencies(spec, task):
    """
    Get yaml spec dependencies from a single yaml spec.
    """
    spec_dependencies = spec.get("dependencies", [])
    task_dependencies = task.dependencies if hasattr(task, "dependencies") else []

    return spec_dependencies + task_dependencies

# External Dependencies

def get_spec_external_dependencies(spec):
    external_dependencies = {}
    external_dependencies["task_id"] = spec["task_id"]
    external_dependencies["external_dependencies"] = spec["external_dependencies"] if "external_dependencies" in spec.keys() else None
    return external_dependencies

############################
## Dependency Aggregation ##
############################

def get_external_dependencies(yaml_specs):
    external_dependencies = [*map(get_spec_external_dependencies, yaml_specs)]
    external_dependencies = [external_dependency for external_dependency in external_dependencies if external_dependency["external_dependencies"] is not None]
    return external_dependencies

######################
## Set Dependencies ##
######################

def set_dependencies(yaml_specs, tasks, latest_only=True, **kwargs):
    dependencies = [(s["task_id"], get_yaml_spec_dependencies(s, tasks[s["task_id"]]))
        for s in yaml_specs]

    external_dependencies = get_external_dependencies(yaml_specs)

    if latest_only:
         latest_only_operator = LatestOnlyOperator(task_id='latest_only', dag=kwargs["dag"])

    #external_dependencies
    external_tasks = {}
    tasks_with_external_dependencies = []

    for task in external_dependencies:
        task_id = task["task_id"]
        tasks_with_external_dependencies.append(task_id)
        external_dependencies = task["external_dependencies"]

        for external_dependency in external_dependencies:
            external_dag, external_task = list(external_dependency.items())[0]
            wait_for_whole_dag = external_task == r"all"
            task_name = "wait_for_DAG_" + external_dag if wait_for_whole_dag else "wait_for_" + external_task

            if task_name not in external_tasks.keys():
                if wait_for_whole_dag:
                    wait_for_task = ExternalTaskSensor(dag = kwargs["dag"],
                                                       task_id = task_name,
                                                       external_dag_id=external_dag,
                                                       external_task_id=None,
                                                       poke_interval=20,
                                                       timeout=60,
                                                       retries=25)
                    external_tasks[task_name] = wait_for_task
                else:
                    wait_for_task = ExternalTaskSensor(dag = kwargs["dag"],
                                                       task_id = task_name,
                                                       external_dag_id=external_dag,
                                                       external_task_id=external_task,
                                                       poke_interval=60,
                                                       timeout=60,
                                                       retries=25)
                    external_tasks[task_name] = wait_for_task

                if latest_only:
                    wait_for_task.set_upstream(latest_only_operator)

            tasks[task_id].set_upstream(external_tasks[task_name])

    # local dependencies
    for task_id, task_depends_on in dependencies:
        valid_dependencies = [d for d in task_depends_on
            if d in tasks.keys() and d != task_id]

        if len(valid_dependencies) == 0 and latest_only and task_id not in tasks_with_external_dependencies:
            tasks[task_id].set_upstream(latest_only_operator)
        else:
            for d in valid_dependencies:
                tasks[task_id].set_upstream(tasks[d])



###############################################
################# Build Tasks #################
###############################################

def build_tasks(yaml_specs, dag):
    task_dict = {}

    for spec in yaml_specs:
        operator = __get_operator(spec["operator"])

        # The spec will have dag added and some keys removed
        args = {k:v for k,v in spec.items()
            if not hasattr(operator, 'template_fields')
                or k in operator.template_fields}
        args["task_id"] = spec["task_id"]
        args["dag"] = dag

        # Some arguments are used only by gusty
        for field in ['operator', 'dependencies', 'external_dependencies']:
            args.pop(field, None)

        task = operator(**args)

        task_dict[spec["task_id"]] = task

    return task_dict

def build_dag(directory, dag, latest_only=True):
    yaml_specs = get_yaml_specs(directory)
    tasks = build_tasks(yaml_specs, dag=dag)
    set_dependencies(yaml_specs, tasks, dag=dag, latest_only=latest_only)

    return tasks

###############################################
################## GustyDAG ###################
###############################################

class GustyDAG(airflow.DAG):
    """
    A version of an Airflow DAG that is created from a directory
    of spec files, generally YAML, Rmd, or Jupyter notebooks.
    Arguments to the DAG can be given either as keyword arguments
    or in a METADATA.yml file.
    """
    def __init__(self, directory, latest_only = True, **kwargs):
        name = os.path.basename(directory)
        
        metadata_file = os.path.join(directory, "METADATA.yml")
        if os.path.exists(metadata_file):
            with open(metadata_file) as inf:
                dag_metadata = yaml.load(inf, GustyYAMLLoader)

                # The keyword arguments take precedence over metadata,
                # except that default_args is also combined
                default_args = dag_metadata.get("default_args", {})
                default_args.update(kwargs.get("default_args", {}))
                
                dag_metadata.update(kwargs)
                kwargs = dag_metadata
                kwargs["default_args"] = default_args

        # Initialize the DAG
        super(GustyDAG, self).__init__(name, **kwargs)

        # Create dependencies
        yaml_specs = get_yaml_specs(directory)
        tasks = build_tasks(yaml_specs, dag=self)
        set_dependencies(yaml_specs, tasks, dag=self, latest_only=latest_only)
