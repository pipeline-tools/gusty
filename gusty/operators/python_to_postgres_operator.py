import os
import re
from inflection import underscore

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.utils.decorators import apply_defaults

from gusty.templates.sql_templates import postgres_comment_table

def clean_columns(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.map(lambda x: underscore(x))
    df.columns = df.columns.map(lambda x: re.sub('\'', '', x))
    df.columns = df.columns.map(lambda x: re.sub('\"', '', x))
    df.columns = df.columns.map(lambda x: re.sub('[^0-9a-zA-Z_]+', '_', x))
    df.columns = df.columns.map(lambda x: re.sub('_+', '_', x))
    return df

class PythonToPostgresOperator(BaseOperator):
    """Upload a pandas DataFrame, from a get_data method, to a Postgres database"""
    ui_color = "#fffad8"
    template_fields = BaseOperator.template_fields + ["postgres_conn_id", "schema", "description", "fields"]

    @apply_defaults
    def __init__(
            self,
            postgres_conn_id = "postgres_default",
            schema = "views",
            description = None,
            fields = None,
            **kwargs):

        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.description = description
        self.fields = fields
        super(PythonToPostgresOperator, self).__init__(**kwargs)

    def get_data(self, context):
        raise NotImplementedError("A PythonToPostgresOperator should " +
          "implement a get_data method that returns a pandas DataFrame")

    def execute(self, context):
        # Get dataset from the get_data method
        dataset = self.get_data(context)
        dataset_cleaned = clean_columns(dataset)

        hook = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()

        # upload it to SQL
        dataset_cleaned.to_sql(name=self.task_id,
                               con=engine,
                               schema=self.schema,
                               if_exists="replace",
                               index=False)
        
        # comment the table
        if self.fields or self.description:
            comment_sql = postgres_comment_table.render(task_id = self.task_id,
                                                        schema = self.schema,
                                                        fields = self.fields,
                                                        description = self.description)

            engine.execute(comment_sql, autocommit = True)
