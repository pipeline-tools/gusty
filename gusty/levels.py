import os
import yaml
import inspect
import airflow
from airflow import DAG
from gusty.utils import GustyYAMLLoader
from gusty import valid_extensions, airflow_version, read_yaml_spec, get_operator

if airflow_version > 1:
    from airflow.utils.task_group import TaskGroup


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
        if not hasattr(operator, "template_fields")  # can this line be removed?
        or k in operator.template_fields
        or k
        in inspect.signature(airflow.models.BaseOperator.__init__).parameters.keys()
    }
    args["task_id"] = spec["task_id"]
    args["dag"] = get_top_level_dag(full_schematic)
    if airflow_version > 1:
        level_structure = get_level_structure(level_id, full_schematic)
        if isinstance(level_structure, TaskGroup):
            args["task_group"] = level_structure

    for field in ["operator", "dependencies", "external_dependencies"]:
        args.pop(field, None)

    task = operator(**args)

    return task


class Level:
    def __init__(
        self,
        full_schematic,
        parent_id,
        name,
        structure,
        metadata,
        dependencies,
        external_dependencies,
    ):
        self.parent_id = parent_id
        self.name = name
        self.is_top_level = self.parent_id is None
        self.metadata = metadata
        self.structure = structure

        if self.is_top_level:
            if os.path.exists(self.metadata or ""):
                with open(self.metadata) as inf:
                    level_metadata = yaml.load(inf, GustyYAMLLoader)
            else:
                level_metadata = None
            self.structure = DAG(self.name, **level_metadata)

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
            if os.path.exists(self.metadata or ""):
                with open(self.metadata) as inf:
                    level_metadata = yaml.load(inf, GustyYAMLLoader)

                    # scrub for TaskGroup inits only
                    level_init_data = {
                        k: v
                        for k, v in level_metadata.items()
                        if k
                        in k
                        in inspect.signature(TaskGroup.__init__).parameters.keys()
                    }
                    level_defaults.update(level_init_data)

                    # add level dependencies if any are found
                    self.level_dependencies = {
                        k: v
                        for k, v in level_metadata.items()
                        if k in k in ["dependencies", "external_dependencies"]
                    }

            self.structure = TaskGroup(**level_defaults)


class GustySetup:
    def __init__(self, dag_dir):
        """
        Get a sense of what the DAG's final structure should look like
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
                    if file.endswith(valid_extensions) and file != "METADATA.yml"
                ],
                "specs": [],
                "metadata": os.path.abspath(os.path.join(dir, "METADATA.yml"))
                if "METADATA.yml" in files
                else None,
                "tasks": {},
                "dependencies": []
                if os.path.basename(os.path.dirname(dir))
                != os.path.basename(os.path.dirname(dag_dir))
                else None,
                "external_dependencies": [],
            }
            for dir, subdirs, files in os.walk(dag_dir)
        }

        # We will accept multiple levels only for Airflow v2 and up
        # This will keep the TaskGroup logic of the Levels class
        # Solely for Airflow v2 and up
        self.levels = [level_id for level_id in self.schematic.keys()]
        self.levels = [self.levels[0]] if airflow_version < 2 else self.levels

        self.wait_for_tasks = None
        self.latest_only_task = None

    def create_level(self, id):
        """
        A level is a structure, such as a DAG or a TaskGroup, which may have dependencies or external_dependencies defined in its METADATA.yml.
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

        # add any dependencies from level creation
        if "level_dependencies" in level.__dict__.keys():
            self.schematic[id].update(level.level_dependencies)

    def read_specs(self, id):
        """
        For a given level id, parse all of that level's yaml specs, given paths to those files.
        """
        level_spec_paths = self.schematic[id]["spec_paths"]
        level_specs = [read_yaml_spec(spec_path) for spec_path in level_spec_paths]
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

    def create_level_dependencies(self, id):
        """
        For a given level id, identify what would be considered a valid set of dependencies within the dag
        for that level, and then set any specified dependencies upstream of that level. An example here would be
        a DAG has two .yml jobs and one subfolder, which we turn into a TaskGroup. That TaskGroup can validly depend on
        the two .yml jobs, so long as either of those task_ids are defined within the dependencies section of the TaskGroup's
        METADATA.yml
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
        level_specs = self.schematic[id]["specs"]
        level_tasks = self.schematic[id]["tasks"]
        sibling_levels = {
            level["name"]: level["structure"]
            for level_id, level in self.schematic.items()
            if level["parent_id"] == id and level_id != id
        }
        valid_dependency_objects = {**level_tasks, **sibling_levels}
        for task_id, task in level_tasks.items():
            task_spec_dependencies = {
                task_id: spec["dependencies"]
                for spec in level_specs
                if spec["task_id"] == task_id and "dependencies" in spec.keys()
            }

            if len(task_spec_dependencies) > 0:
                task_dependencies = [
                    dependency for dependency in task_spec_dependencies[task_id]
                    if dependency in valid_dependency_objects.keys()
                    and dependency != task_id
                ]
                for dependency in task_dependencies:
                    task.set_upstream(valid_dependency_objects[dependency])

    # TODO
    #   - wait for tasks
    #   - latest only
    #   - task groups accept renaming?
