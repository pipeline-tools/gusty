---
operator: airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator
date: "2022-04-01"
doc: "{{simple_constructor()}}"
---

SELECT date FROM my_table WHERE date = {{ date }}
