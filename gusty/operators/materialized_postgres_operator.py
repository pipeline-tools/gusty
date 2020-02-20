import re

from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults

from ..templates.sql_templates import postgres_create_table, postgres_comment_table

def detect_dependencies(query, schema, task_id):
    query = re.sub(re.compile(r'\/\*.*\*\/', re.MULTILINE), "", query)
    query = re.sub("--.*\n", "", query)
    query = re.sub(re.compile(r'[\s]+', re.MULTILINE), " ", query)

    regex = "[^a-z\d_\.]" + schema + "\.([a-z\d_\.]*)"
    query_tables = re.finditer(regex, query)
    ret = list(set(m.group(1) for m in query_tables))

    return ret

###############
## Operators ##
###############

class MaterializedPostgresOperator(PostgresOperator):
    ui_color = "#c37ed5"
    template_fields = PostgresOperator.template_fields + ("schema", "description", "fields", )

    @apply_defaults
    def __init__(
            self,
            task_id,
            sql,
            postgres_conn_id = "postgres_default",
            schema = "views",
            description = None,
            fields = None,
            **kwargs):

        self.schema = schema
        self.description = description
        self.fields = fields

        # Turn the SQL into a CREATE TABLE + document command
        create_sql = postgres_create_table.render(task_id = task_id,
                                                  schema = schema,
                                                  sql = sql)
        doc_sql = postgres_comment_table.render(task_id = task_id,
                                             schema = schema,
                                             description = description,
                                             fields = fields)
        combined_sql = create_sql + "\n" + doc_sql
        
        # Automatically detect dependencies (to be resolved later)
        self.dependencies = detect_dependencies(sql, schema, task_id)

        super(MaterializedPostgresOperator, self).__init__(
            task_id = task_id,
            sql = combined_sql,
            postgres_conn_id = postgres_conn_id,
            **kwargs)
