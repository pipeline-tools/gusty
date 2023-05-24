dag_contents_map = {
    "hi.py": (
        "# ---\n"
        "# python_callable: say_hi\n"
        "# ---\n"
        "\n"
        "def say_hi():\n"
        '  greeting = "hi"\n'
        "  print(greeting)\n"
        "  return greeting\n"
    ),
    "hey.sql": (
        "---\n"
        "operator: airflow.providers.sqlite.operators.sqlite.SqliteOperator\n"
        "---\n"
        "\n"
        "SELECT 'hey'\n"
    ),
    "hello.yml": (
        "operator: airflow.operators.bash.BashOperator\n" "dependencies:\n",
        "  - hi\n",
        "bash_command: echo hello\n",
    ),
    "METADATA.yml": (
        'description: "Saying hello using different file types"\n',
        "doc_md: |-\n",
        "  This is a longform description,\n",
        "  which can be accessed from Airflow's\n",
        "  Graph view for your DAG. It looks\n",
        "  like a tiny poem.\n",
        'schedule: "0 0 * * *"\n',
        "catchup: False\n",
        "default_args:\n",
        "  owner: You\n",
        "  email: you@you.com\n",
        "  start_date: !days_ago 28\n",
        "  email_on_failure: True\n",
        "  email_on_retry: False\n",
        "  retries: 1\n",
        "  retry_delay: !timedelta \n",
        "    minutes: 5\n",
        "tags:\n",
        "  - docs\n",
        "  - demo\n",
        "  - hello\n",
    ),
}

create_dag_file = lambda dag_name: (
    f"create_{dag_name}.py",
    (
        "import os\n",
        "from gusty import create_dag\n",
        "\n",
        "# There are many different ways to find Airflow's DAGs directory.\n",
        f'# gusty_dag_dir returns something like: "/usr/local/airflow/dags/{dag_name}"\n',
        f'gusty_dag_dir = os.path.join(os.environ.get("AIRFLOW_HOME"), "dags/{dag_name}")\n'
        "\n",
        "gusty_dag = create_dag(gusty_dag_dir, latest_only=False)\n",
    ),
)

create_dags_file = lambda dag_folder_name: (
    f"create_{dag_folder_name}.py",
    (
        "import os\n",
        "from gusty import create_dags\n",
        "from gusty.utils import days_ago\n",
        "\n",
        f'# gusty_dags_dir returns something like: "/usr/local/airflow/dags/{dag_folder_name}"\n',
        f'gusty_dags_dir = os.path.join(os.environ.get("AIRFLOW_HOME"), "dags/{dag_folder_name}")\n',
        "\n",
        "create_dags(\n",
        "  gusty_dags_dir,\n",
        "  globals(),\n",
        '  schedule="0 0 * * *",\n',
        "  catchup=False,\n",
        "  default_args={\n",
        '    "owner": "you",\n',
        '    "email": "you@you.com",\n',
        '    "start_date": days_ago(1)\n',
        "  },\n",
        "  wait_for_defaults={\n",
        '    "mode": "reschedule"\n',
        "  },\n",
        '  extra_tags=["gusty_dags"],\n',
        "  latest_only=False\n" ")\n",
    ),
)
