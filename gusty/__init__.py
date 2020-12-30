import airflow
from gusty.building import GustyBuilder


def create_dag(
    dag_dir, task_group_defaults={}, wait_for_defaults={}, latest_only=True, **kwargs
):
    setup = GustyBuilder(
        dag_dir,
        task_group_defaults=task_group_defaults,
        wait_for_defaults=wait_for_defaults,
        latest_only=latest_only,
        **kwargs
    )
    [setup.parse_metadata(level) for level in setup.levels]
    [setup.create_structure(level) for level in setup.levels]
    [setup.read_specs(level) for level in setup.levels]
    [setup.create_tasks(level) for level in setup.levels]
    [setup.create_level_dependencies(level) for level in setup.levels]
    [setup.create_task_dependencies(level) for level in setup.levels]
    [setup.create_task_external_dependencies(level) for level in setup.levels]
    [setup.create_level_external_dependencies(level) for level in setup.levels]
    [setup.create_root_dependencies(level) for level in setup.levels]
    return setup.return_dag()
