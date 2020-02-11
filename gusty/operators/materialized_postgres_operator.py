import re

from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults

from ..templates.sql_templates import postgres_create_table, postgres_comment_table

###############
## Operators ##
###############

class MaterializedPostgresOperator(PostgresOperator):
    ui_color = "#c37ed5"

    @apply_defaults
    def __init__(
            self,
            task_id,
            sql,
            schema = "views",
            postgres_conn_id = "postgres_default",
            description = None,
            fields = None,
            **kwargs):

        # Turn the SQL into a CREATE TABLE + document command
        create_sql = postgres_create_table.render(task_id = task_id,
                                                  schema = schema,
                                                  sql = sql)
        doc_sql = render_document_sql.render(task_id = task_id,
                                             schema = schema,
                                             description = description,
                                             fields = fields)
        combined_sql = create_sql + "\n" + doc_sql

        super(MaterializedPostgresOperator, self).__init__(
            task_id = task_id,
            sql = combined_sql,
            postgres_conn_id = postgres_conn_id,
            **kwargs)
