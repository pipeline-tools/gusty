---
operator: airflow.providers.standard.operators.empty.EmptyOperator
start_date: !days_ago 24
end_date: !custom_days_ago
---

SELECT *
FROM somewhere
