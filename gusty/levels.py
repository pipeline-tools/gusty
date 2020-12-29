import os
import yaml
import inspect
import airflow
from airflow import DAG
from gusty.utils import GustyYAMLLoader
from gusty import valid_extensions, airflow_version, read_yaml_spec, get_operator

if airflow_version > 1:
    from airflow.utils.task_group import TaskGroup

if airflow_version > 1:
    from airflow.operators.latest_only import LatestOnlyOperator
    from airflow.sensors.external_task import ExternalTaskSensor
else:
    from airflow.operators.latest_only_operator import LatestOnlyOperator
    from airflow.sensors.external_task_sensor import ExternalTaskSensor


def get_level_structure(level_id, full_schematic):
    level_structure = full_schematic[level_id]["structure"]
    return level_structure


def get_top_level_dag(full_schematic):
    top_level_id = list(full_schematic.keys())[0]
    top_level_dag = get_level_structure(top_level_id, full_schematic)
    return top_level_dag


def build_task(spec, level_id, full_schematic):
    operator = get_operator(spec["operator"])
    args = {
        k: v
        for k, v in spec.items()
        if k in operator.template_fields
        or k
        in inspect.signature(airflow.models.BaseOperator.__init__).parameters.keys()
    }
    args["task_id"] = spec["task_id"]
    args["dag"] = get_top_level_dag(full_schematic)
    if airflow_version > 1:
        level_structure = get_level_structure(level_id, full_schematic)
        if isinstance(level_structure, TaskGroup):
            args["task_group"] = level_structure

    task = operator(**args)

    return task


class Level:
    def __init__(
        self,
        full_schematic,
        parent_id,
        name,
        metadata,
    ):
        self.parent_id = parent_id
        self.name = name
        self.is_top_level = self.parent_id is None
        self.metadata = metadata

        if self.is_top_level:
            if self.metadata is not None:
                level_init_data = {
                    k: v
                    for k, v in self.metadata.items()
                    if k in k in inspect.signature(DAG.__init__).parameters.keys()
                }
            self.structure = DAG(self.name, **level_init_data)

        else:
            # What is the main DAG?
            top_level_dag = get_top_level_dag(full_schematic)

            # What is the parent structure?
            parent = full_schematic[self.parent_id]["structure"]

            # Set some TaskGroup defaults
            level_defaults = {
                "group_id": self.name,
                "prefix_group_id": False,
                "dag": top_level_dag,
            }

            # If the parent structure is another TaskGroup, add it as parent_group kwarg
            if isinstance(parent, TaskGroup):
                level_defaults.update({"parent_group": parent})

            # Read in any metadata
            if self.metadata is not None:
                # scrub for TaskGroup inits only
                level_init_data = {
                    k: v
                    for k, v in self.metadata.items()
                    if k in k in inspect.signature(TaskGroup.__init__).parameters.keys()
                    and k not in ["dag", "parent_group"]
                }
                level_defaults.update(level_init_data)

            self.structure = TaskGroup(**level_defaults)


class GustySetup:
    def __init__(self, dag_dir, **kwargs):
        """
        Because DAGs can be multiple "levels" now, the Setup class is here to first
        create a "schematic" of the DAG's levels and all of the specs associated with
        each level, at which point it moves through the schematic to build each level's
        "structure" (DAG or TaskGroup), tasks, dependencies, and external_dependencies
        """
        self.schematic = {
            # Each entry is a "level" of the main DAG
            os.path.abspath(dir): {
                "name": os.path.basename(dir),
                "parent_id": os.path.abspath(os.path.dirname(dir))
                if os.path.basename(os.path.dirname(dir))
                != os.path.basename(os.path.dirname(dag_dir))
                else None,
                "structure": None,
                "spec_paths": [
                    os.path.abspath(os.path.join(dir, file))
                    for file in files
                    if file.endswith(valid_extensions)
                    and file != "METADATA.yml"
                    and not file.startswith(("_", "."))
                ],
                "specs": [],
                "metadata_path": os.path.abspath(os.path.join(dir, "METADATA.yml"))
                if "METADATA.yml" in files
                else None,
                "metadata": {},
                "tasks": {},
                "dependencies": []
                if os.path.basename(os.path.dirname(dir))
                != os.path.basename(os.path.dirname(dag_dir))
                else None,
                "external_dependencies": [],
            }
            for dir, subdirs, files in os.walk(dag_dir)
            if not os.path.basename(dir).startswith(("_", "."))
        }

        # store all defaults
        self.defaults = kwargs if len(kwargs) > 0 else {}

        # handle external dependency / wait_for defaults
        self.wait_for_defaults = {"poke_interval": 10, "timeout": 60, "retries": 60}
        if "wait_for_defaults" in self.defaults.keys():
            user_wait_for_defaults = {
                k: v
                for k, v in self.defaults["wait_for_defaults"].items()
                if k
                in [
                    "poke_interval",
                    "timeout",
                    "retries",
                    "soft_fail",
                    "execution_date_fn",
                    "check_existence",
                ]
            }
            self.wait_for_defaults.update(user_wait_for_defaults)

        # We will accept multiple levels only for Airflow v2 and up
        # This will keep the TaskGroup logic of the Levels class
        # Solely for Airflow v2 and up
        self.levels = [level_id for level_id in self.schematic.keys()]
        self.levels = [self.levels[0]] if airflow_version < 2 else self.levels

        # For tasks gusty creates outside of specs provided by the directory
        # It is important for gusty to keep a record  of the tasks created
        self.wait_for_tasks = {}
        self.latest_only_task = {}
        self.all_tasks = {}

    def parse_metadata(self, id):
        metadata_defaults = self.defaults.copy()
        # if top-level DAG, get rid of task_group_defaults and wait_for_defaults
        if self.schematic[id]["parent_id"] is None:
            metadata_defaults.pop("task_group_defaults", None)
            metadata_defaults.pop("wait_for_defaults", None)
        # otherwise, only keep task_group_defaults
        else:
            metadata_defaults = (
                metadata_defaults["task_group_defaults"]
                if "task_group_defaults" in metadata_defaults.keys()
                else {}
            )
        # METADATA.yml will override defaults
        level_metadata_path = self.schematic[id]["metadata_path"]
        if os.path.exists(level_metadata_path or ""):
            with open(level_metadata_path) as inf:
                level_metadata = yaml.load(inf, GustyYAMLLoader)
        else:
            level_metadata = {}
        metadata_defaults.update(level_metadata)
        level_metadata = metadata_defaults
        self.schematic[id]["metadata"] = level_metadata
        # dependencies get explicity set at the level-"level" for each level
        level_dependencies = {
            k: v
            for k, v in level_metadata.items()
            if k in k in ["dependencies", "external_dependencies"]
        }
        if len(level_dependencies) > 0:
            self.schematic[id].update(level_dependencies)

    def create_level(self, id):
        """
        Given a level of the DAG, a structure such as a DAG or a TaskGroup will be initialized
        and, additionally, dependencies or external dependencies specified in METADATA.yml will be
        added to the level's schematic
        """
        level_schematic = self.schematic[id]
        level_kwargs = {
            k: v
            for k, v in level_schematic.items()
            if k in inspect.signature(Level.__init__).parameters.keys()
        }

        # The Level class instantiates the structure (a DAG or TaskGroup)
        # And checks METADATA.yml for dependencies if any are present
        level = Level(self.schematic, **level_kwargs)

        # We update the schematic with the structure of the level created
        self.schematic[id].update({"structure": level.structure})

    def read_specs(self, id):
        """
        For a given level id, parse all of that level's yaml specs, given paths to those files.
        """
        level_metadata = self.schematic[id]["metadata"]
        level_spec_paths = self.schematic[id]["spec_paths"]
        level_specs = [read_yaml_spec(spec_path) for spec_path in level_spec_paths]
        if airflow_version > 1:
            level_structure = self.schematic[id]["structure"]
            level_name = self.schematic[id]["name"]
            if isinstance(level_structure, TaskGroup):
                if level_structure.prefix_group_id:
                    for level_spec in level_specs:
                        level_spec["task_id"] = "{x}_{y}".format(
                            x=level_name, y=level_spec["task_id"]
                        )
                elif "suffix_group_id" in level_metadata.keys():
                    if level_metadata["suffix_group_id"]:
                        for level_spec in level_specs:
                            level_spec["task_id"] = "{y}_{x}".format(
                                x=level_name, y=level_spec["task_id"]
                            )
        self.schematic[id].update({"specs": level_specs})

    def create_tasks(self, id):
        """
        For a given level id, create all tasks based on the specs parsed from read_specs
        """
        level_specs = self.schematic[id]["specs"]
        level_tasks = {
            spec["task_id"]: build_task(spec, id, self.schematic)
            for spec in level_specs
        }
        self.schematic[id]["tasks"] = level_tasks
        self.all_tasks.update(level_tasks)

    def create_level_dependencies(self, id):
        """
        For a given level id, identify what would be considered a valid set of dependencies within the dag
        for that level, and then set any specified dependencies upstream of that level. An example here would be
        a DAG has two .yml jobs and one subfolder, which (the subfolder) is turned into a TaskGroup. That TaskGroup
        can validly depend on the two .yml jobs, so long as either of those task_ids are defined within the dependencies
        section of the TaskGroup's METADATA.yml
        """
        level_structure = self.schematic[id]["structure"]
        level_dependencies = self.schematic[id]["dependencies"]
        level_parent_id = self.schematic[id]["parent_id"]
        if level_parent_id is not None:
            level_parent = self.schematic[level_parent_id]
            parent_tasks = level_parent[
                "tasks"
            ]  # these follow the format {task_id: task_object}
            sibling_levels = {
                level["name"]: level["structure"]
                for level_id, level in self.schematic.items()
                if level["parent_id"] == level_parent_id and level_id != id
            }  # these follow the format {level_name: structure_object}
            valid_dependency_objects = {**parent_tasks, **sibling_levels}
            for dependency in level_dependencies:
                if dependency in valid_dependency_objects.keys():
                    level_structure.set_upstream(valid_dependency_objects[dependency])

    def create_task_dependencies(self, id):
        """
        For a given level id, identify what would be considered a valid set of dependencies within the dag
        for that level, and then set any specified dependencies for each task at that level, as specified by
        the task's specs.
        """
        level_specs = self.schematic[id]["specs"]
        level_tasks = self.schematic[id]["tasks"]
        sibling_levels = {
            level["name"]: level["structure"]
            for level_id, level in self.schematic.items()
            if level["parent_id"] == id and level_id != id
        }
        valid_dependency_objects = {**self.all_tasks, **sibling_levels}
        for task_id, task in level_tasks.items():
            task_dependencies = (
                task.dependencies if hasattr(task, "dependencies") else []
            )
            spec_dependencies = {
                task_id: spec["dependencies"]
                for spec in level_specs
                if spec["task_id"] == task_id and "dependencies" in spec.keys()
            }
            if len(spec_dependencies) > 0:
                spec_dependencies = spec_dependencies[task_id]
                spec_task_dependencies = task_dependencies + spec_dependencies
            else:
                spec_task_dependencies = task_dependencies

            spec_task_dependencies = list(set(spec_task_dependencies))

            # Should tasks be able to depend on things on any level?
            spec_task_dependencies = [
                dependency
                for dependency in spec_task_dependencies
                if dependency in valid_dependency_objects.keys()
            ]

            if len(spec_task_dependencies) > 0:
                for dependency in spec_task_dependencies:
                    task.set_upstream(valid_dependency_objects[dependency])

    def create_task_external_dependencies(self, id):
        """
        As with create_task_dependencies, for a given level id, parse all of the external dependencies in a task's
        spec, then create and add those "wait_for_" tasks upstream of a given task. Note the Setup class must keep
        a record of all "wait_for_" tasks as to not recreate the same task twice.
        """
        level_specs = self.schematic[id]["specs"]
        level_tasks = self.schematic[id]["tasks"]
        for task_id, task in level_tasks.items():
            task_spec_external_dependencies = {
                task_id: spec["external_dependencies"]
                for spec in level_specs
                if spec["task_id"] == task_id and "external_dependencies" in spec.keys()
            }
            if len(task_spec_external_dependencies) > 0:
                task_external_dependencies = [
                    external_dependency
                    for external_dependency in task_spec_external_dependencies[task_id]
                ]
                task_external_dependencies = dict(
                    j for i in task_external_dependencies for j in i.items()
                )
                for (
                    external_dag_id,
                    external_task_id,
                ) in task_external_dependencies.items():
                    wait_for_task_name = (
                        "wait_for_DAG_{x}".format(x=external_dag_id)
                        if external_task_id == "all"
                        else "wait_for_{x}".format(x=external_task_id)
                    )
                    if wait_for_task_name in self.wait_for_tasks.keys():
                        wait_for_task = self.wait_for_tasks[wait_for_task_name]
                        task.set_upstream(wait_for_task)
                    else:
                        wait_for_task = ExternalTaskSensor(
                            dag=get_top_level_dag(self.schematic),
                            task_id=wait_for_task_name,
                            external_dag_id=external_dag_id,
                            external_task_id=(
                                external_task_id if external_task_id != "all" else None
                            ),
                            **self.wait_for_defaults
                            # maybe give users options to set these sensor parameters?
                            # poke_interval=10,
                            # timeout=60,
                            # retries=60,
                        )
                        self.wait_for_tasks.update({wait_for_task_name: wait_for_task})
                        task.set_upstream(wait_for_task)

    def create_level_external_dependencies(self, id):
        level_structure = self.schematic[id]["structure"]
        level_external_dependencies = self.schematic[id]["external_dependencies"]
        level_parent_id = self.schematic[id]["parent_id"]
        if level_parent_id is not None:
            if len(level_external_dependencies) > 0:
                level_external_dependencies = dict(
                    j for i in level_external_dependencies for j in i.items()
                )
                for (
                    external_dag_id,
                    external_task_id,
                ) in level_external_dependencies.items():
                    wait_for_task_name = (
                        "wait_for_DAG_{x}".format(x=external_dag_id)
                        if external_task_id == "all"
                        else "wait_for_{x}".format(x=external_task_id)
                    )
                    if wait_for_task_name in self.wait_for_tasks.keys():
                        wait_for_task = self.wait_for_tasks[wait_for_task_name]
                        level_structure.set_upstream(wait_for_task)
                    else:
                        wait_for_task = ExternalTaskSensor(
                            dag=get_top_level_dag(self.schematic),
                            task_id=wait_for_task_name,
                            external_dag_id=external_dag_id,
                            external_task_id=(
                                external_task_id if external_task_id != "all" else None
                            ),
                            # maybe give users options to set these sensor parameters?
                            **self.wait_for_defaults
                            # poke_interval=10,
                            # timeout=60,
                            # retries=60,
                        )
                        self.wait_for_tasks.update({wait_for_task_name: wait_for_task})
                        level_structure.set_upstream(wait_for_task)

    def create_root_dependencies(self, id):
        """
        Finally, we look at the root for latest only and any external dependencies (TODO) at the root level.
        Then we wire up any tasks and groups from the parent level to these deps if needed
        """
        level_metadata = self.schematic[id]["metadata"]
        level_parent_id = self.schematic[id]["parent_id"]
        # parent level only
        if level_parent_id is None:
            if level_metadata is not None:
                level_latest_only = (
                    level_metadata["latest_only"]
                    if "latest_only" in level_metadata.keys()
                    else True
                )
            else:
                level_latest_only = True
            if level_latest_only:
                latest_only_operator = LatestOnlyOperator(
                    task_id="latest_only", dag=get_top_level_dag(self.schematic)
                )
                level_tasks = self.schematic[id]["tasks"]
                child_levels = {
                    level["name"]: level["structure"]
                    for level_id, level in self.schematic.items()
                    if level["parent_id"] == id
                }
                valid_dependency_objects = {
                    **level_tasks,
                    **child_levels,
                    **self.wait_for_tasks,
                }
                for name, dependency in valid_dependency_objects.items():
                    if len(dependency.upstream_task_ids) == 0:
                        dependency.set_upstream(latest_only_operator)

    def return_dag(self):
        return get_top_level_dag(self.schematic)


def create_DAG(
    dag_dir,
    task_group_defaults={},
    wait_for_defaults={},
    latest_only=True,
    **kwargs
):
    setup = GustySetup(
        dag_dir,
        task_group_defaults=task_group_defaults,
        wait_for_defaults=wait_for_defaults,
        latest_only=latest_only,
        **kwargs
    )
    [setup.parse_metadata(level) for level in setup.levels]
    [setup.create_level(level) for level in setup.levels]
    [setup.read_specs(level) for level in setup.levels]
    [setup.create_tasks(level) for level in setup.levels]
    [setup.create_level_dependencies(level) for level in setup.levels]
    [setup.create_task_dependencies(level) for level in setup.levels]
    [setup.create_task_external_dependencies(level) for level in setup.levels]
    [setup.create_level_external_dependencies(level) for level in setup.levels]
    [setup.create_root_dependencies(level) for level in setup.levels]
    return setup.return_dag()
