# Provide plugins through airflow.operators.gusty

from airflow.plugins_manager import AirflowPlugin
from .operators.rmd_operator import RmdOperator
from .operators.jupyter_operator import JupyterOperator
from .operators.materialized_postgres_operator import MaterializedPostgresOperator
from .operators.csv_to_postgres_operator import CSVToPostgresOperator

class GustyPlugin(AirflowPlugin):
  name = 'gusty'
  operators = [RmdOperator, JupyterOperator, MaterializedPostgresOperator,
               CSVToPostgresOperator]
