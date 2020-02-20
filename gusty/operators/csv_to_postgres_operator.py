import os

from airflow.utils.decorators import apply_defaults

from .python_to_postgres_operator import PythonToPostgresOperator

class CSVToPostgresOperator(PythonToPostgresOperator):
    """Upload a CSV file from the dags/csv folder to a Postgres connection."""
    ui_color = "#fffad8"
    template_fields = PythonToPostgresOperator.template_fields + ["csv_file"]

    @apply_defaults
    def __init__(
            self,
            csv_file,
            **kwargs):
        self.csv_file = csv_file
        super(CSVToPostgresOperator, self).__init__(**kwargs)

    def get_data(self, context):
        """Read in a CSV file from dags/csv, and upload it to Postgres"""
        csv_dir = os.path.join(os.getenv('AIRFLOW_HOME'), "dags", "csv")
        csv_path = os.path.join(csv_dir, self.csv_file)
    
        if not os.path.exists(csv_path):
            raise FileNotFoundError("CSV %s not found in %s" %
            (self.csv_file, csv_dir))

        import pandas as pd
        csv_data = pd.read_csv(csv_path)
        return csv_data
