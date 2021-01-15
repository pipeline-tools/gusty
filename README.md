# gusty

[![Versions](https://img.shields.io/badge/python-3.6+-blue)](https://pypi.org/project/gusty/)
[![PyPi](https://img.shields.io/pypi/v/gusty.svg)](https://pypi.org/project/gusty/)
![build](https://github.com/chriscardillo/gusty/workflows/build/badge.svg)
[![coverage](https://codecov.io/github/chriscardillo/gusty/coverage.svg?branch=master)](https://codecov.io/github/chriscardillo/gusty?branch=master)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

gusty allows you to control your Airflow DAGs, Task Groups, and Tasks with greater ease. gusty manages collections of tasks, represented as any number of YAML, Python, Jupyter Notebook, or R Markdown files. A directory of task files is instantly rendered into a DAG by passing a file path to gusty's `create_dag` function.

gusty also manages dependencies (within one DAG) and external dependencies (dependencies on tasks in other DAGs) for each task file you define. All you have to do is provide a list of `dependencies` or `external_dependencies` inside of a task file, and gusty will automatically set each task's dependencies and create external task sensors for any external dependencies listed.

gusty works with both Airflow 1.x and Airflow 2.x, and has even more features, all of which aim to make the creation, management, and iteration of DAGs more fluid, so that you can intuitively design your DAG and build your tasks.

## What's in gusty?

### Four Ways to Make Tasks

gusty will turn every file in a DAG directory into a task. gusty supports four different file types, which offer convenient ways to specify an operator and operator parameters for task creation.

| File Type | How It Works                                                                                                   |
| --------- | -------------------------------------------------------------------------------------------------------------- |
| .yml      | Declare an `operator` and pass in any operator parameters using YAML                                           |
| .py       | Simply define a function named `python_callable` and gusty will automatically turn it into a `PythonOperator`  |
| .ipynb    | Put a YAML block at the top of your notebook and specify an `operator` that renders your Jupyter Notebook      |
| .Rmd      | Use the YAML block at the top of your notebook and specify an `operator` that renders your R Markdown Document |

Here is quick example of a YAML task file, which might be called something like `hello_world.yml`:

```yml
operator: airflow.operators.bash.BashOperator
bash_command: echo hello world
```

The resulting task would be a `BashOperator` with the task id `hello_world`.

Here is the same approach using a Python file instead, named `hello_world.py`, which gusty will automatically turn into a `PythonOperator`:

```py
def python_callable():
  phrase = "hello world"
  print(phrase)
```

### Easy Dependencies

Every task file type supports `dependencies` and `external_dependencies` parameters, which gusty will use to automatically assign dependencies between tasks and create external task sensors for any external dependencies listed for a given task.

For .yml, .ipynb, and .Rmd task file types, dependencies and external_dependencies would be defined using YAML syntax:

```yml
operator: airflow.operators.bash.BashOperator
bash_command: echo hello world
dependencies:
  - same_dag_task
external_dependencies:
  - another_dag: another_task
  - a_whole_dag: all
```

For external dependencies, the keyword `all` can be used when the task should wait on an entire external DAG to run successfully.

For a .py task file type, we can define these dependencies simply as variables:

```py
dependencies = [
  "same_dag_task"
]
external_dependencies = [
  {"another_dag": "another_task"},
  {"a_whole_dag": "all"}
]
def python_callable():
  phrase = "hello world"
  print(phrase)
```

### DAG and Task Group Control

Both DAG and TaskGroup objects are created automatically simply by being directories and subfolders, respectively. The directory path you provide to gusty's `create_dag` function will become your DAG (and DAG name), and any subfolder in that DAG by default will be turned into a Task Group.

gusty offers a few compatible methods for configuring DAGs and Task Groups that we'll cover below.

#### Metadata

A special file name in any directory or subfolder is `METADATA.yml`, which gusty will use to determine

Here is an example of a `METADATA.yml` file you might place in a DAG directory:

```yml
description: "An example of a DAG created using METADATA.yml"
schedule_interval: "1 0 * * *"
default_args:
    owner: airflow
    depends_on_past: False
    start_date: !days_ago 1
    email: airflow@example.com
    email_on_failure: False
    email_on_retry: False
    retries: 1
    retry_delay: !timedelta 'minutes: 5'
```

And here is an example of a `METADATA`.yml file you might place in a TaskGroup subfolder:

```yml
tooltip: "This is a task group tooltip"
prefix_group_id: True
dependencies:
  - hello_world
```

As seen in the above example, gusty will also accept `dependencies` and `external_dependencies` in a Task Group's `METADATA.yml`. This means gusty can wire up your Task Group dependencies as well!

Note that by default, gusty disables the TaskGroup `prefix_group_id` argument by default, as it's one of gusty's few opinions that tasks should explicitly named unless you say otherwise. gusty also offers a `suffix_group_id` argument for Task Groups!

#### create_dag

While `METADATA.yml` will always be the primary source of truth for a DAG or TaskGroup's configuration, gusty's `create_dag` function also accepts any parameters that can be passed to Airflow's DAG class, as well as a dictionary of `task_group_defaults` to set default behavior for any Task Group created by gusty.

Here's an example of using `create_dag`, where instead of metadata we use `create_dag` arguments:

```py
import airflow
from datetime import timedelta
from airflow.utils.dates import days_ago
from gusty import create_dag

dag = create_dag(
  '/usr/local/airflow/dags/hello_world',
  description="A dag created without any metadata",
  schedule_interval="1 0 * * *",
  default_args={
      "owner": "airflow",
      "depends_on_past": False,
      "start_date": days_ago(1),
      "email": "airflow@example.com",
      "email_on_failure": False,
      "email_on_retry": False,
      "retries": 1,
      "retry_delay": timedelta(minutes=5),
  },
  task_group_defaults={
      "tooltip": "This is a task group tooltip",
      "prefix_group_id": True
  }
)
```

You might notice that `task_group_defaults` does not include dependencies. For Task Groups, dependencies must be set using Task Group-specific metadata.

Default arguments in `create_dag` and a DAG or Task Group's `METADATA.yml` can be mixed and matched. `METADATA.yml` will always override defaults set in `create_dag`.

#### DAG-level Features

gusty features some additional helpful features at the DAG-level to help you design your DAGs with ease:

  - **root_tasks** - A list of task ids which should represent the roots of a DAG. For example, an HTTP sensor might have to succeed before any downstream tasks in the DAGs run.
  - **leaf_tasks** - A list of task ids which should represent the leaves of a DAG. For example, at the end of the DAG run, you might save a report to S3.
  - **external_dependencies** - You can also set external dependencies at the DAG level! Making your DAG wait on other DAGs works just like in the external dependencies examples above.
  - **ignore_subfolders** - If you don't want subfolders to generate Task Groups, set this to `True`.
  - **latest_only** - On by default, installs a LatestOnlyOperator at the absolute root of the DAG, skipping all tasks in the DAG if the DAG run is not the current run. You can read more about the LatestOnlyOperator in [Airflow's documentation](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#latest-run-only).

  Any of arguments can be placed in `create_dag` or `METADATA.yml`!


### Local Operator Support

gusty also works with your local operators, so long as they are located inside of an `operators` folder located inside of your `AIRFLOW_HOME`.

In order for gusty to support your operators as expected, your operator name must be CamelCase and the file in which it lives must be snake_case.

For example, if we wanted use a `HelloOperator`, this operator would need to be stored in a file called `hello_operator.py` in inside of the `operators` folder located inside of your `AIRFLOW_HOME`.

Any fields from your operator's `__init__` method will be passed from gusty to your operator. So if your `HelloOperator` had a `name` field, you could call this operator with a YAML task file that looks something like this:

```yml
operator: local.HelloOperator
name: World
```

The `local.` syntax is what gusty uses to know to look in your local operators folder for the operator.

## Containerized Demo

As an additional resource, you can check out a containerized demo of gusty and Airflow over at the [gusty-demo repo](https://github.com/chriscardillo/gusty-demo), which illustrates how gusty and a few custom operators can make SQL queries, Jupyter notebooks, and RMarkdown documents all work together in the same data pipeline.
