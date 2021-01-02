# gusty

[![Versions](https://img.shields.io/badge/python-3.6+-blue)](https://pypi.org/project/gusty/)
[![PyPi](https://img.shields.io/pypi/v/gusty.svg)](https://pypi.org/project/gusty/)
![build](https://github.com/chriscardillo/gusty/workflows/build/badge.svg)
[![coverage](https://codecov.io/github/chriscardillo/gusty/coverage.svg?branch=master)](https://codecov.io/github/chriscardillo/gusty?branch=master)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

gusty allows you to manage your Airflow DAGs, tasks, and task groups with greater ease. It can automatically generate dependencies between tasks and external dependencies for tasks in other DAGs.

The gusty approach to Airflow is that individual tasks are represented as YAML, where an operator and its arguments, along with its dependencies and external dependencies, are specified in a `.yml` file. By passing a directory path of these YAML task specifications to gusty's `create_dag` function, you can have your DAGs create themselves.

In addition to parsing YAML files, gusty also parses YAML front matter in `.ipynb` and `.Rmd` files, allowing you to include Python and R notebook formats in your data pipeline straightaway.

Lastly, gusty's `create_dag` function can be passed any keyword argument from Airflow's DAG class, as well as dictionaries for task group defaults and external dependency sensor defaults. And if you'd rather, gusty can pick up per-DAG and per-task-group specifications via YAML files titled `METADATA.yml` - which will override any defaults passed to `create_dag` - so you can specify defaults *and* override those defaults with metadata.

gusty works with both Airflow 1.x and Airflow 2.x, and automatically generates task groups in Airflow 2.x. Plus, you can specify task group dependencies and external_dependencies in each task group's `METADATA.yml` file.

In short, gusty allows you to focus on the tasks in a pipeline instead of the scaffolding.

## Up and running with `create_dag`

gusty's core function is `create_dag`. To have gusty generate a DAG, provide a path to a directory that contains `.yml` files for the DAG's tasks. The `create_dag` function can take any keyword arguments from Airflow's DAG class, as well as dictionaries for task group defaults (`task_group_defaults`) and external dependency sensor defaults (`wait_for_defaults`).

An example of the entire `.py` file that generates your DAG looks like this:

```py
import airflow
from datetime import timedelta
from airflow.utils.dates import days_ago
from gusty import create_dag

dag = create_dag(
  '/usr/local/airflow/dags/hello_world',
  description="A dag created without metadata",
  schedule_interval="0 0 * * *",
  default_args={
      "owner": "gusty",
      "depends_on_past": False,
      "start_date": days_ago(1),
      "email": "gusty@gusty.com",
      "email_on_failure": False,
      "email_on_retry": False,
      "retries": 3,
      "retry_delay": timedelta(minutes=5),
  },
  task_group_defaults={"prefix_group_id": True},
  wait_for_defaults={"retries": 10},
  latest_only=False
  )
```

Note you must import Airflow.

The resulting DAG will be named after the directory, in this case, `hello_world`. By default, gusty will create a `latest_only` DAG, where every job in the DAG will only run for the most recent run date, regardless of if a backfill is called. This behavior is disabled above using `latest_only=False`. As with anything in gusty, all of these parameters can also be specified in `METADATA.yml` files.

Examples how to create a directory of DAG task files can be found in [the gusty repo's example folder](https://github.com/chriscardillo/gusty/tree/master/examples).

## Containerized Demo

As an additional resource, you can check out a containerized demo of gusty and Airflow over at the [gusty-demo repo](https://github.com/chriscardillo/gusty-demo), which illustrates how gusty and a few custom operators can make SQL queries, Jupyter notebooks, and RMarkdown documents all work together in the same data pipeline.

For more details on the gusty package, please see below.

## Hello World in YAML

### YAML Tasks

Instead of importing and calling a `BashOperator` directly, you can specify the operator and the `bash_command` parameter (which is a required field for Airflow's BashOperator) in a `.yml` file:

```yml
operator: airflow.operators.bash.BashOperator
bash_command: echo hello world
```

gusty takes this `.yml` and turns it into a task based on its file name. If this file was called `hello_world.yml`, the resulting task would show up in your DAG as `hello_world`.

### Dependencies

You can also set dependencies between jobs in `.yml` as well. Here is another task, `goodbye_world`. that depends on `hello_world`.

```yml
operator: airflow.operators.bash.BashOperator
bash_command: echo goodbye world
dependencies:
  - hello_world
```

This will automatically set the `goodbye_world` task downstream of the `hello_world` task.

External dependencies for tasks in other DAGs can be set using the `dag_id: task_id` format:

```yml
external_dependencies:
  - dag_id: task_id
```

To wait for an entire external DAG to run successfully just use the format `dag_id: all` instead.

### DAGs as YAML

As mentioned, your DAGs can also be represented as `.yml` files. Specifically, DAGs should be represented in a file called `METADATA.yml`. Similar to the basic Airflow tutorial, our DAG's `.yml` might look something like this:

```yml
description: "A Gusty version of the DAG described by this Airflow tutorial: https://airflow.apache.org/docs/stable/tutorial.html"
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
#   queue: bash_queue
#   pool: backfill
#   priority_weight: 10
#   end_date: !datetime [2016, 1, 1]
#   wait_for_downstream: false
#   sla: !timedelta 'hours: 2'
#   trigger_rule: all_success
```

### Task Groups

As of Airflow 2.0.0, task groups provide Airflow users with another layer of organization. gusty fully supports task groups, and creating a task group is as easy as adding a new folder within your DAG's main folder, and adding task `.yml` files to that subfolder. gusty will take care of the rest.

By default, gusty turns off prefixing task names with task group names, but you can enable this functionality by either adding `prefix_group_id: True` to a task group's `METADATA.yml`, or adding `task_group_defaults={"prefix_group_id": True}` to your call to `create_dag`. As mentioned, you can set defaults in your call to `create_dag`, then override those defaults using per-task-group `METADATA.yml` files.

gusty also accepts a `suffix_group_id` parameter, which will place the task group name at the end of the task name, if that's what you want!

In short, if it's available in a task group, it's available in gusty.

### External Task Sensors

When you specify external dependencies, gusty will use Airflow's `ExternalTaskSensor` to create `wait_for_` tasks in your DAG. Using the `wait_for_defaults` parameter in `create_dag`, you can specify the behavior of these `ExternalTaskSensor` tasks, things like `mode` ("poke"/"reschedule") and `poke_interval`.

You can also specify external dependencies at the DAG level if you want, and gusty will ensure that DAG-level external dependencies sit at the root of your DAG.

### Root Tasks

gusty also features the ability for you to specify "root tasks" for your DAG, where a root task is defined as "some task that should happen before any other task in the DAG". To enable this, you just have to provide a list of `root_tasks` to the DAG's `METADATA.yml` or in `create_dag`. Root tasks will only work if they have no upstream or downstream dependencies, which enables gusty to place these tasks at the root of your DAG.

## Operators

### Calling Airflow Operators

gusty will take parameterized `.yml` for any operator, given a string that includes the module path and the operator class, such as `airflow.operators.bash.BashOperator` or `airflow.providers.amazon.aws.transfers.s3_to_redshift.S3ToRedshiftOperator`. In theory, if it's available in a module, you can use a `.yml` to define it.

Since sensors are also operators, you can utilize them with gusty, too!

### Calling Custom Operators

gusty will also work with any of your custom operators, so long as those operators are located in an `operators` directory in your designated `AIRFLOW_HOME`.

In order for your local operators to import properly, they must follow the pattern of having a snake_case file name and a CamelCase operator name, for example the filename of an operator called `YourOperator` must be called `your_operator.py`.

Just as the BashOperator above was accessed via with full module path prepended, `airflow.operators.bash.BashOperator`, your local operators are accessed via the `local` keyword, e.g. `local.YourOperator`.
