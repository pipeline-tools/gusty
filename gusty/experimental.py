import os
import asyncio
from gusty import create_dag


async def create_dag_async(dag_dir, **kwargs):
    return {
        "dag_id": os.path.basename(dag_dir),
        "dag": create_dag(dag_dir, **kwargs),
    }


async def create_dags_async(
    gusty_dags, globals, concurrent_timeout, max_concurrency, **kwargs
):
    async_results = asyncio.as_completed(
        [create_dag_async(gusty_dag, **kwargs) for gusty_dag in gusty_dags],
        timeout=concurrent_timeout,
    )
    sem = asyncio.Semaphore(max_concurrency)
    async with sem:
        for async_dag in async_results:
            try:
                dag_dict = await async_dag
                globals[dag_dict["dag_id"]] = dag_dict["dag"]
            except asyncio.exceptions.TimeoutError:
                raise asyncio.exceptions.TimeoutError(
                    f"create_dags took more than {concurrent_timeout} seconds. "
                    "To fix, adjust the concurrent_timeout in the call to create_dags, "
                    "or set a higher AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT in your environment."
                )


def create_dags(
    dags_dir,
    globals,
    concurrent=False,
    concurrent_timeout=None,
    max_concurrency=8,
    **kwargs,
):
    # assumes any subdirectories in the dags directory are
    # gusty DAGs (excludes subdirectories like __pycache__)
    gusty_dags = [
        os.path.join(dags_dir, name)
        for name in os.listdir(dags_dir)
        if os.path.isdir(os.path.join(dags_dir, name)) and not name.endswith("__")
    ]

    if concurrent:
        if concurrent_timeout is None:
            concurrent_timeout = (
                float(os.environ.get("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", 30.0))
                * 0.9
            )
        asyncio.run(
            create_dags_async(
                gusty_dags, globals, concurrent_timeout, max_concurrency, **kwargs
            )
        )

    else:
        for gusty_dag in gusty_dags:
            dag_id = os.path.basename(gusty_dag)
            globals[dag_id] = create_dag(gusty_dag, **kwargs)
