import os
import re
from inflection import underscore

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.utils.decorators import apply_defaults

from gusty.templates.sql_templates import postgres_comment_table

#############
## Globals ##
#############

csv_dir = os.path.join(os.getenv('AIRFLOW_HOME'), "dags", "csv")

###############
## Functions ##
###############

def clean_columns(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.map(lambda x: underscore(x))
    df.columns = df.columns.map(lambda x: re.sub('\'', '', x))
    df.columns = df.columns.map(lambda x: re.sub('\"', '', x))
    df.columns = df.columns.map(lambda x: re.sub('[^0-9a-zA-Z_]+', '_', x))
    df.columns = df.columns.map(lambda x: re.sub('_+', '_', x))
    return df

def upload_csv(csv_file, table_name, schema, engine):
    csv_path = os.path.join(csv_dir, csv_file)
    
    if not os.path.exists(csv_dir):
        raise FileNotFoundError("CSV %s should be in airflow/dags/csv" % (csv_file, ))

    import pandas as pd
    csv_file = pd.read_csv(csv_path)
    csv_file = clean_columns(csv_file)
    csv_file.to_sql(name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists="replace",
                    index=False)

class CSVToPostgresOperator(BaseOperator):
    """Upload a CSV file from the dags/csv folder to a Postgres connection."""
    ui_color = "#fffad8"
    template_fields = BaseOperator.template_fields + ["csv_file", "postgres_conn_id", "schema", "description", "fields", ]

    @apply_defaults
    def __init__(
            self,
            csv_file,
            postgres_conn_id = "postgres_default",
            schema = "views",
            description = None,
            fields = None,
            **kwargs):

        self.csv_file = csv_file
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.description = description
        self.fields = fields
        super(CSVToPostgresOperator, self).__init__(**kwargs)

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()

        upload_csv(self.csv_file, self.task_id, self.schema, engine)
        
        # comment the table
        if self.fields or self.description:
            comment_sql = postgres_comment_table.render(task_id = self.task_id,
                                                        schema = self.schema,
                                                        fields = self.fields,
                                                        description = self.description)

            engine.execute(comment_sql, autocommit = True)

