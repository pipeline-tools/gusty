---
operator: airflow.operators.dummy.DummyOperator
start_date: !days_ago 24
end_date: !custom_days_ago
---

SELECT *
FROM somewhere
