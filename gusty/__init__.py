import airflow
from gusty.building import GustyBuilder


def create_dag(
    dag_dir,
    task_group_defaults={},
    wait_for_defaults={},
    latest_only=True,
    parse_hooks={},
    dag_constructors={},
    **kwargs
):
    """
    Create an Airflow DAG by passing a directory of .yml task files.

    Each .yml task file should represent one task in the DAG. .ipynb and .Rmd files with yaml frontmatter are also
    accepted by create_dag. Any DAG kwargs (e.g. schedule_interval, description) can be passed directly into create_dag.
    Additionally, if your directory contains multiple subdirectories of .yml task files, and you are on Airflow V2, create_dag
    will create task groups for each subdirectory. By default, prefix_group_id is set to False.

    Parameters:
        dag_dir (str): A path to a directory of .yml task files. .ipynb and .Rmd files with yaml frontmatter are also accepted
        task_group_defaults (dict): Any parameter that could be passed to Airflow's TaskGroup class
        wait_for_defaults (dict): Any parameter that could be passed to Airflow's ExternalTaskSensor (which is what gusty users to create external_dependency tasks)
        latest_only (bool): When True, creates a latest_only operator at the root of the DAG, which will ensure tasks only run for the most recent run date
        parse_hooks (dict): Keys are file extensions (beginning with periods), values are functions that parse a given file path for a the associated file extension.
        kwargs: Any additional keyword argument that can be passed to Airflow's DAG class, e.g. default_args, schedule_interval, description

    Returns:
        dag (airflow.DAG): A fully initialized Airflow DAG, complete with tasks, task groups, dependencies, external dependencies, and latest_only operator (if wanted).
    """

    setup = GustyBuilder(
        dag_dir,
        task_group_defaults=task_group_defaults,
        wait_for_defaults=wait_for_defaults,
        latest_only=latest_only,
        parse_hooks=parse_hooks,
        dag_constructors=dag_constructors,
        **kwargs
    )
    [setup.parse_metadata(level) for level in setup.levels]
    [setup.check_metadata(level) for level in setup.levels]
    [setup.create_structure(level) for level in setup.levels]
    [setup.read_specs(level) for level in setup.levels]
    [setup.create_tasks(level) for level in setup.levels]
    [setup.create_level_dependencies(level) for level in setup.levels]
    [setup.create_task_dependencies(level) for level in setup.levels]
    [setup.create_task_external_dependencies(level) for level in setup.levels]
    [setup.create_level_external_dependencies(level) for level in setup.levels]
    [setup.create_leaf_tasks(level) for level in setup.levels]
    [setup.create_root_dependencies(level) for level in setup.levels]
    return setup.return_dag()
