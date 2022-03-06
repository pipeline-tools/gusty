from gusty.importing import airflow_version

if airflow_version > 1:
    from airflow.sensors.external_task import ExternalTaskSensor
else:
    from airflow.sensors.external_task_sensor import ExternalTaskSensor


def create_external_sensor(
    dag,
    external_dag_id,
    external_task_id,
    wait_for_defaults,
    wait_for_overrides,
    wait_for_task_name,
    sensor=ExternalTaskSensor,
):
    ext_task_defaults = wait_for_defaults.copy()
    ext_task_overrides = wait_for_overrides.get(external_dag_id, {})
    # Only one of execution_delta or
    # execution_date_fn can be passed to
    # the ExternalTaskSensor
    if "execution_delta" in ext_task_overrides:
        if "execution_date_fn" in ext_task_defaults:
            ext_task_defaults.pop("execution_date_fn")
    ext_task_defaults.update(ext_task_overrides)
    wait_for_task = sensor(
        dag=dag,
        task_id=wait_for_task_name,
        external_dag_id=external_dag_id,
        external_task_id=(external_task_id if external_task_id != "all" else None),
        **ext_task_defaults
    )

    return wait_for_task


def make_external_task_name(external_dag_id, external_task_id):
    return (
        "wait_for_DAG_{x}".format(x=external_dag_id)
        if external_task_id == "all"
        else "wait_for_{x}".format(x=external_task_id)
    )
