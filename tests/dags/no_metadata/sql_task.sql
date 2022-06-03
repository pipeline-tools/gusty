---
operator: airflow.providers.sqlite.operators.sqlite.SqliteOperator
date: "2022-04-01"
doc: "{{simple_constructor()}}"
---

SELECT date FROM my_table WHERE date = {{ date }}
