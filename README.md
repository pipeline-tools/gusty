![logo](https://raw.githubusercontent.com/chriscardillo/gusty/main/images/logo_500.svg?raw=true)

[![Versions](https://img.shields.io/badge/python-3.6+-blue)](https://pypi.org/project/gusty/)
[![PyPi](https://img.shields.io/pypi/v/gusty.svg)](https://pypi.org/project/gusty/)
![build](https://github.com/chriscardillo/gusty/workflows/build/badge.svg)
[![coverage](https://codecov.io/github/chriscardillo/gusty/coverage.svg?branch=main)](https://codecov.io/github/chriscardillo/gusty?branch=main)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

gusty allows you to control your Airflow DAGs, Task Groups, and Tasks with greater ease. gusty manages collections of tasks, represented as any number of YAML, Python, SQL, Jupyter Notebook, or R Markdown files. A directory of task files is instantly rendered into a DAG by passing a file path to gusty's `create_dag` function.

gusty also manages dependencies (within one DAG) and external dependencies (dependencies on tasks in other DAGs) for each task file you define. All you have to do is provide a list of `dependencies` or `external_dependencies` inside of a task file, and gusty will automatically set each task's dependencies and create external task sensors for any external dependencies listed.

gusty works with both Airflow 1.x and Airflow 2.x, and has even more features, all of which aim to make the creation, management, and iteration of DAGs more fluid, so that you can intuitively design your DAG and build your tasks.

## What's in gusty?

### Five Ways to Make Tasks

gusty will turn every file in a DAG directory into a task. By default gusty supports five different file types, which offer convenient ways to specify an operator and operator parameters for task creation.

| File Type | How It Works                                                                                                                  |
| --------- | ----------------------------------------------------------------------------------------------------------------------------- |
| .yml      | Declare an `operator` and pass in any operator parameters using YAML                                                          |
| .py       | Simply write Python code and by default gusty will execute your file using a `PythonOperator`. Other options available        |
| .sql      | Declare an `operator` in a YAML header, then write SQL in the main .sql file. The SQL automatically gets sent to the operator |
| .ipynb    | Put a YAML block at the top of your notebook and specify an `operator` that renders your Jupyter Notebook                     |
| .Rmd      | Use the YAML block at the top of your notebook and specify an `operator` that renders your R Markdown Document                |

Here is quick example of a YAML task file, which might be called something like `hello_world.yml`:

```yml
operator: airflow.operators.bash.BashOperator
bash_command: echo hello world
```

The resulting task would be a `BashOperator` with the task id `hello_world`.

Here is the same approach using a Python file instead, named `hello_world.py`, which gusty will automatically turn into a `PythonOperator` by default:

```py
phrase = "hello world"
print(phrase)
```

Lastly, here's a slightly different `.sql` example:

```sql
---
operator: airflow.providers.sqlite.operators.sqlite.SqliteOperator
---

SELECT
  column_1,
  column_2
FROM your_table
```

### Easy Dependencies

#### Declarative Dependencies

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

For a .py task file type, we can define these dependencies with some raw markdown at the top of the file:

```py
# ---
# dependencies:
#   - same_dag_task
# external_dependencies:
#   - another_dag: another_task
#   - a_whole_dag: all
# python_callable: say_hello
# ---

def say_hello():
  phrase = "hello world"
  print(phrase)
```

You will also note that we wrapped our previous Python code in a function called `say_hello`, and passed this function's name to the `python_callable` argument. By default, with no `operator` and no `python_callable` specified, gusty will pass a simple function that runs your .py file to the `PythonOperator`. If you pass an explicit `python_callable` by name, gusty will search your .py for that function and pass that function to the `PythonOperator` instead.

.py files are capable of accepting an `operator` parameter in the raw markdown, just like any other task file type, which means you can use any other relevant operators (e.g. the `PythonVirtualenvOperator`) to execute your Python code as needed.


#### Dynamic Dependencies

gusty can also detect and generate dependencies through a task object's `dependencies` attribute. This means you can also **dynamically** set dependencies. One popular example of this option would be if your operator runs SQL, you can parse that SQL for table names, and attach a list of those table names to the operator's `dependencies` attribute. If those table names listed in the `dependencies` attribute are also task ids in the DAG, gusty will be able to automatically set these dependencies for you!

### DAG and TaskGroup Control

Both DAG and TaskGroup objects are created automatically simply by being directories and subfolders, respectively. The directory path you provide to gusty's `create_dag` function will become your DAG (and DAG name), and any subfolder in that DAG by default will be turned into a TaskGroup.

gusty offers a few compatible methods for configuring DAGs and Task Groups that we'll cover below.

#### Metadata

A special file name in any directory or subfolder is `METADATA.yml`, which gusty will use to determine how to configure that DAG or TaskGroup object.

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

As seen in the above example, gusty will also accept `dependencies` and `external_dependencies` in a TaskGroup's `METADATA.yml`. This means gusty can wire up your TaskGroup dependencies as well!

Note that gusty disables the TaskGroup `prefix_group_id` argument by default, as it's one of gusty's few opinions that tasks should explicitly named unless you say otherwise. gusty also offers a `suffix_group_id` argument for Task Groups!

#### create_dag

While `METADATA.yml` will always be the primary source of truth for a DAG or TaskGroup's configuration, gusty's `create_dag` function also accepts any parameters that can be passed to Airflow's DAG class, as well as a dictionary of `task_group_defaults` to set default behavior for any TaskGroup created by gusty.

Here's an example using `create_dag`, where instead of metadata we use `create_dag` arguments:

```py
import airflow
from datetime import timedelta
from airflow.utils.dates import days_ago
from gusty import create_dag

dag = create_dag(
  '/usr/local/airflow/dags/hello_world',
  description="A DAG created without any metadata",
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

You might notice that `task_group_defaults` does not include dependencies. For Task Groups, dependencies must be set using TaskGroup-specific metadata.

Default arguments in `create_dag` and a DAG or TaskGroup's `METADATA.yml` can be mixed and matched. `METADATA.yml` will always override defaults set in `create_dag`.

#### create_dags

If you have multiple gusty DAGs located inside of a single directory, you can conveniently use the `create_dags` (plural) function.

`create_dags` works just like `create_dag`, with two exceptions:

1. The first argument to `create_dags` is the path to a directory with many gusty DAGs.

2. The second argument to `create_dags` is `globals()`. `globals()` is essentially the namespace to which your DAGs are assigned.

Let's adjust the above `create_dag` example to use `create_dags` instead:

```py
import airflow
from datetime import timedelta
from airflow.utils.dates import days_ago
from gusty import create_dags

create_dags(
  '/usr/local/airflow/my_gusty_dags',
  globals(),
  description="A default description for my DAGs.",
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

The above will create many gusty DAGs located in the `/usr/local/airflow/my_gusty_dags` directory.

#### DAG-level Features

gusty features additional helpful arguments at the DAG-level to help you design your DAGs with ease:

  - **`root_tasks`** - A list of task ids which should represent the roots of a DAG. For example, an HTTP sensor might have to succeed before any downstream tasks in the DAG run.
  - **`leaf_tasks`** - A list of task ids which should represent the leaves of a DAG. For example, at the end of the DAG run, you might save a report to S3.
  - **`external_dependencies`** - You can also set external dependencies at the DAG level! Making your DAG wait on other DAGs works just like in the external dependencies examples above.
  - **`ignore_subfolders`** - If you don't want subfolders to generate Task Groups, set this to `True`.
  - **`latest_only`** - On by default, installs a `LatestOnlyOperator` at the absolute root of the DAG, skipping all tasks in the DAG if the DAG run is not the current run. You can read more about the LatestOnlyOperator in [Airflow's documentation](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/latest_only/index.html?highlight=latestonly#airflow.operators.latest_only.LatestOnlyOperator).

  Any of these arguments can be placed in `create_dag` or `METADATA.yml`!


### Local Operator Support

While you can store your local operators in Airflow's `plugins` directory and reference an operator's `plugins` path accordingly, gusty also allows you to alternatively keep local operators inside of an `operators` folder located inside of your `AIRFLOW_HOME`.

In order for gusty to support your operators as expected, your operator name must be CamelCase and the file in which the operator lives must be snake_case.

For example, if we wanted use a `HelloOperator`, this operator would need to be stored in a file called `hello_operator.py` in inside of the `operators` folder located inside of your `AIRFLOW_HOME`.

Any fields from your operator's `__init__` method will be passed from gusty to your operator. So if your `HelloOperator` had a `name` field, you could call this operator with a YAML task file that looks something like this:

```yml
operator: local.HelloOperator
name: World
```

The `local.` syntax is what gusty uses to know to look in your local operators folder for the operator.

### Multi-Task Generation

Sometimes task definitions can be repetitive. To account for this, gusty allows for a `multi_task_spec` block in any frontmatter. This allows you to generate multiple similar tasks with a single task definition file! For example, let's say you wanted to create two bash tasks, each containing a different `bash_command`. You can define these two tasks in a single task definition file like so:

```yml
operator: airflow.operators.bash.BashOperator
multi_task_spec:
  bash_task_1:
    bash_command: echo first_task
  bash_task_2:
    bash_command: echo second_task
```

gusty will convert the above into two task instances, `bash_task_1` and `bash_task_2`, each with a unique `bash_command`.

Additionally, for the special case of `python_callable` in a .py file, you can specify `python_callable_partials`:

```py
# ---
# python_callable: main
# python_callable_partials:
#   python_task_1:
#       my_kwarg: a
#   python_task_2:
#       my_kwarg: b
# ---


def main(my_kwarg):
    return my_kwarg
```

gusty will convert the above in two task instances, `python_task_1` and `python_task_2`. `python_task_1` will return `"a"` and `python_task_2` will return `"b"`.

`multi_task_spec` and `python_callable_partials` are non-exclusive, so you can mix and match configuration as needed.

## One Approach, Not the Only Approach

One good thing about gusty is that if you choose to use this package, gusty doesn't have to be the **only** way that you create DAGs. You can use gusty's `create_dag` to generate DAGs out of directories where applicable, and then implement more traditional methods of creating Airflow DAGs where the tried and true methods feel like a better approach.

So feel free to give the gusty approach a try, because you don't have to commit to it everywhere. But when you try it, don't be surprised if you start using it everywhere!

## Containerized Demo

As an additional resource, you can check out a containerized demo of gusty and Airflow over at the [gusty-demo repo](https://github.com/chriscardillo/gusty-demo), which illustrates how gusty and a few custom operators can make SQL queries, Jupyter notebooks, and RMarkdown documents all work together in the same data pipeline.


## Development

The below assumes you have [Docker](https://www.docker.com/) installed.

### Starting for the First Time

First, `git clone` this repository.

Then:

```bash
export GUSTY_DEV_HOME="~/path/to/this/project"
cd $GUSTY_DEV_HOME
make build-image
make run-image
```

The above will build the development image under the name `gusty-testing` and run a container called `gusty-testing`.

From here, you can:

- `make exec` - Exec into a terminal in the running container.
- `make test` - Runs `pytest` in a temporary container.
- `make coverage` - Runs `pytest` and generates a coverage report.
- `make browse-coverage` - Opens up the coverage report in your browser.
- `make stop-container` - Stop the running container.
- `make start-container` - Start a stopped container.

### Rebuilding the Image

```bash
make stop-container # if you have a running container
make remove-container # if you have a stopped container
make build-image
make run-image
```
