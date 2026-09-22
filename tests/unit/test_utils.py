import importlib.util
from pathlib import Path
from typing import Protocol, cast

from airflow import DAG

DAG_FILE = (
    Path(__file__).resolve().parents[2]
    / 'airflow'
    / 'dags'
    / 'financial_data_dag.py'
)


class DagModule(Protocol):
    dag: DAG

def _load_dag_module(path: Path) -> DagModule:
    spec = importlib.util.spec_from_file_location(
        'financial_data_dag',
        path
    )

    if spec is None or spec.loader is None:
        raise ImportError(f'Could not load DAG from {path}')

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return cast(DagModule, module)

def load_dag() -> DAG:
    dag_module = _load_dag_module(DAG_FILE)
    return dag_module.financial_dag