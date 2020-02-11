# gusty

Gusty is an opinionated framework for data ETL built on top of Airflow, where every task is represented by one YAML file, and each task creates a view in a database.

## Structure

The `.yml` approach to generating jobs within Airflow DAGs is not a new idea, but it is useful and there are a few built in benefits to it here.

- **Dependencies** - Dependencies can quickly be set in `.yml` files through one of three means:

    1. Using the `dependencies` specification, you can set dependencies between jobs in the same DAG.
    2. Using the `external_dependencies` specification, you can set dependencies between jobs in different DAGs.
    3. For the `MaterializedPostgresOperator`, dependencies in the same DAG that are a part of the `views` schema are automatically registered.

- **Operator configuration** - After you build an operator, you can pass parameters to it in each `.yml` job definition file. This means that, for example, if you have to call different API endpoints, you may only need to build one operator to ingest data from this API, and then can specify the endpoint to call in the `.yml` job definition file.

- **Support for popular notebook formats** - There are currently two **notebook** operators, `RmdOperator` and `JupyterOperator`, which enable you to simply write RMarkdown or Jupyter Notebook files and deploy them as jobs in your data pipeline. More importantly, `RmdOperator` and `JupyterOperator` are actually executed on separate dedicated docker containers, and interact with the Airflow container via SSH, which is useful if you want to deploy these services separately in the cloud!
