# gusty

[![Versions](https://img.shields.io/badge/python-3.6+-blue)](https://pypi.org/project/gusty/)
[![PyPi](https://img.shields.io/pypi/v/gusty.svg)](https://pypi.org/project/gusty/)
![build](https://github.com/chriscardillo/gusty/workflows/build/badge.svg)
[![coverage](https://codecov.io/github/chriscardillo/gusty/coverage.svg?branch=master)](https://codecov.io/github/chriscardillo/gusty?branch=master)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

gusty allows you to manage your Airflow DAGs, tasks, and task groups with greater ease. It can automatically generate dependencies between tasks and external dependencies for tasks in other DAGs.

The gusty approach to Airflow is that individual tasks are represented as YAML, where an operator and its arguments, along with dependencies and external dependencies, are specified. By passing a directory path of YAML task specifications to `create_dag`, you can have your DAGs create themselves.

In addition to parsing YAML files, gusty also parses YAML front matter in `.ipynb` and `.Rmd` files, allowing you to include Python and R notebook formats in your data pipeline straightaway.

Lastly, gusty's `create_dag` function can be passed any keyword argument from Airflow's DAG class, as well as dictionaries for task group defaults and external dependency sensor defaults. And if you'd rather, gusty can pick up per-DAG and per-task-group specifications via YAML files titled `METADATA.yml` - which will override any defaults passed to `create_dag` - so you can specify defaults *and* override those defaults with metadata.

gusty works with both Airflow 1.x and Airflow 2.x, and automatically generates task groups in Airflow 2.x. Plus, you can even specify task group dependencies and external external_dependencies in `METADATA.yml`.

In short, gusty allows you to focus on the tasks in a pipeline instead of the scaffolding.

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

### DAGs

As mentioned, your DAGs can also be represented as `.yml` files. Specifically, DAGs should be represented in a file called `METADATA.yml`. Similar to the basic Airflow tutorial, our DAG might look something like this:

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

By default, gusty will create a `latest_only` DAG, where every job in the DAG will only run for the most recent run date, regardless of if a backfill is called. You can disable this behavior by adding `latest_only: False` to the block above. This can also be passed as a keyword argument in `create_dag`

### create_dag

To have gusty generate a DAG, you can use the `create_dag` function, which just needs an (absolute) path to a directory that contains  `.yml` files for the DAG's tasks. If you haven't specified any `METADATA.yml` in the DAG directory, you'll have to pass in DAG-related arguments, as you would with any other use of Airflow's DAG class.

An example of the entire `.py` file that generates your DAG looks like this:

```py
import airflow
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
  latest_only=False
  )
```

Note how you must also import Airflow here. The resulting DAG will be named after the directory, in this case, `hello_world`.

## Operators

### Airflow Operators

gusty will take parameterized `.yml` for any operator, given a string that includes the module path and the operator class, such as `airflow.operators.bash.BashOperator` or `airflow.providers.amazon.aws.transfers.s3_to_redshift.S3ToRedshiftOperator`. In theory, if it's available in a module, you can use a `.yml` to define it.

### Custom Operators

gusty will also work with any of your custom operators, so long as those operators are located in an `operators` directory in your designated `AIRFLOW_HOME`.

In order for your local operators to import properly, they must follow the pattern of having a snake_case file name and a CamelCase operator name, for example the filename of an operator called `YourOperator` must be called `your_operator.py`.

Just as the BashOperator above was accessed via with full module path prepended, `airflow.operators.bash.BashOperator`, your local operators are accessed via the `local` keyword, e.g. `local.YourOperator`.

## Demo

You can use a containerized demo of gusty and Airflow over at the [gusty-demo](https://github.com/chriscardillo/gusty-demo), and see all of the above in practice.
