dag_contents_map = {
    'hi.py': (
        '# ---\n'
        '# python_callable: say_hi\n'
        '# ---\n'
        '\n'
        'def say_hi():\n'
        '\tgreeting = "hi"\n'
        '\tprint(greeting)\n'
        '\treturn greeting\n'
    ),
    'hey.sql': (
        "---\n"
        "operator: airflow.providers.sqlite.operators.sqlite.SqliteOperator\n"
        "---\n"
        "\n"
        "SELECT 'hey'\n"
    ),
    'hello.yml': (
        "operator: airflow.operators.bash.BashOperator\n"
        "bash_command: echo hello\n"
    ),
    'METADATA.yml': (
        'description: "Saying hello using different file types"\n',
        'doc_md: |-\n',
        '\tThis is a longform description,\n',
        '\twhich can be accessed from Airflow\'s\n',
        '\tGraph view for your DAG. It looks\n',
        '\tlike a tiny poem.\n',
        'schedule: "0 0 * * *"\n',
        'catchup: False\n',
        'default_args:\n',
        '\towner: You\n',
        '\temail: you@you.com\n',
        '\tstart_date: !days_ago 28\n',
        '\temail_on_failure: True\n',
        '\temail_on_retry: False\n',
        '\tretries: 1\n',
        '\tretry_delay: !timedelta \n',
        '\tminutes: 5\n',
        'tags:\n',
        '\t- docs\n',
        '\t- demo\n',
        '\t- hello\n'
    )
}

create_dag_file = lambda dag_name: (
    f'create_{dag_name}.py', (
        'import os\n',
        'from gusty import create_dag\n',
        '\n',
        '# There are many different ways to find Airflow\'s DAGs directory.\n',
        '# hello_dag_dir returns something like: "/usr/local/airflow/dags/hello_dag"\n',
        'hello_dag_dir = os.path.join(\n',
        '\tos.environ["AIRFLOW_HOME"], \n',
        '\t"dags", \n',
        '\t"hello_dag"\n',
        ')\n'
        '\n',
        'hello_dag = create_dag(hello_dag_dir, latest_only=False)\n'
    )
)