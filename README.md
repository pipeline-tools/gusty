# gusty

[![Versions](https://img.shields.io/badge/python-3.6+-blue)](https://pypi.org/project/gusty/)
[![PyPi](https://img.shields.io/pypi/v/gusty.svg)](https://pypi.org/project/gusty/)
![build](https://github.com/chriscardillo/gusty/workflows/build/badge.svg)
[![coverage](https://codecov.io/github/chriscardillo/gusty/coverage.svg?branch=master)](https://codecov.io/github/chriscardillo/gusty?branch=master)

gusty allows you to manage your Airflow DAGs and tasks with greater ease. Instead of writing your DAGs, tasks, and dependencies in a `.py` file, you can instead specify DAGs and tasks in `.yml` files, and designate task dependencies within a task's `.yml` file, as well.

In addition to parsing `.yml` files, gusty also parses YAML front matter in `.ipynb` and `.Rmd` files, allowing you to include Python and R notebook formats in your data pipeline.

gusty works with both Airflow 1.x and Airflow 2.x.

## Hello World

### Tasks

Instead of importing and calling a `BashOperator` directly, you can specify the operator and the `command` parameter (which is a required field for Airflow's BashOperator) in a `.yml`:

```yml
operator: airflow.operators.bash.BashOperator
bash_command: echo hello world
```

gusty takes the above `.yml` and turns it into a task based on its file name. If this file was called `hello_world.yml`, the resulting task would show up in your DAG as `hello_world`.

You can also set dependencies between jobs in `.yml` as well. Here is another task, `goodbye_world`. that depends on `hello_world`.

```yml
operator: airflow.operators.bash.BashOperator
bash_command: echo goodbye world
dependencies:
  - hello_world
```

This will automatically set the `goodbye_world` task downstream of the `hello_world` task.

External dependencies can also be set using the format:

```yml
external_dependencies:
  - dag: task
```

To wait for an entire external DAG to run successfully just use `dag: all` instead.

### DAGs

Your DAGs can also be represented as `.yml` files. Specifically, DAGs should be represented in a file called `METADATA.yml`. Similar to the basic Airflow tutorial, our DAG might look something like this:

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
By default, gusty will create a `latest_only` DAG, where every job in the DAG will only run for the most recent run date, regardless of if a backfill is called. You can disable this behavior by adding `latest_only: False` to the `default_args` block above.

### GustyDAG

To have gusty generate a DAG, you can use the `GustyDAG` class, which just needs an (absolute) path to a directory that contains a `METADATA.yml` for the DAG and `.yml` files for the tasks. You must also import airflow. An example of the entire `.py` file that generates your DAG looks like this:

```py
import airflow
from gusty import GustyDAG

dag = GustyDAG('/usr/local/airflow/dags/hello_world')
```
The resulting DAG will be named after the directory, in this case, `hello_world`.

## Operators

### Airflow Operators

gusty will take parameterized `.yml` for any operator located in `airflow.operators` and `airflow.contrib.operators`. In theory, if it's available in these modules, you can use a `.yml` to define it.

### Custom Operators

gusty will also work with any of your custom operators, so long as those operators are located in an `operators` directory in your designated `AIRFLOW_HOME`.

In order for your local operators to import properly, they must follow the pattern of having a snake_case file name and a CamelCase operator name, for example the filename of an operator called `YourOperator` must be called `your_operator.py`.

Just as the BashOperator above was accessed via with full module path prepended, `airflow.operators.bash.BashOperator`, your local operators are accessed via the `local` keyword, e.g. `local.YourOperator`.

## Demo

You can use a containerized demo of gusty and Airflow over at the [gusty-demo](https://github.com/chriscardillo/gusty-demo), and see all of the above in practice.
