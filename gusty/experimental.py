import os
import asyncio
from gusty import create_dag


async def create_dag_async(dag_dir, **kwargs):
    return {
        "dag_id": os.path.basename(dag_dir),
        "dag": create_dag(dag_dir, **kwargs),
    }


async def create_dags_async(gusty_dags, globals, timeout, **kwargs):
    async_dags = [create_dag_async(gusty_dag, **kwargs) for gusty_dag in gusty_dags]
    async_results = asyncio.as_completed(async_dags, timeout=timeout)
    for async_dag in async_results:
        dag_dict = await async_dag
        globals[dag_dict["dag_id"]] = dag_dict["dag"]


def create_dags(dags_dir, globals, parallel=False, timeout=None, **kwargs):
    # assumes any subdirectories in the dags directory are
    # gusty DAGs (excludes subdirectories like __pycache__)
    gusty_dags = [
        os.path.join(dags_dir, name)
        for name in os.listdir(dags_dir)
        if os.path.isdir(os.path.join(dags_dir, name)) and not name.endswith("__")
    ]

    if parallel:
        if timeout is None:
            os.environ.get("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", 30.0) * 0.7
        asyncio.run(create_dags_async(gusty_dags, globals, timeout, **kwargs))

    else:
        for gusty_dag in gusty_dags:
            dag_id = os.path.basename(gusty_dag)
            globals[dag_id] = create_dag(gusty_dag, **kwargs)
