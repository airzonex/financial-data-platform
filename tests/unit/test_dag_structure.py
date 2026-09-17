from datetime import date

from test_utils import load_dag


dag = load_dag()

EXPECTED_TASKS = {
    'create_pipeline_run',
    'prepare_rgbi_run',
    'prepare_keyrate_run',
    'rgbi_ingestion',
    'keyrate_ingestion',
    'rgbi_staging',
    'keyrate_staging',
    'dbt_build',
    'finish_dataset_runs',
    'finish_pipeline_run'
}

EXPECTED_DEPENDENCIES = {
    ('create_pipeline_run', 'prepare_rgbi_run'),
    ('create_pipeline_run', 'prepare_keyrate_run'),

    ('prepare_rgbi_run', 'rgbi_ingestion'),
    ('prepare_keyrate_run', 'keyrate_ingestion'),

    ('rgbi_ingestion', 'rgbi_staging'),
    ('keyrate_ingestion', 'keyrate_staging'),

    ('rgbi_staging', 'dbt_build'),
    ('keyrate_staging', 'dbt_build'),

    ('dbt_build', 'finish_dataset_runs'),
    ('finish_dataset_runs', 'finish_pipeline_run')
}


def test_dag_contains_expected_tasks() -> None:
    actual_tasks = {task.task_id for task in dag.tasks}

    assert actual_tasks == EXPECTED_TASKS

def test_dag_dependencies() -> None:
    actual_dependencies = {
        (upstream.task_id, task.task_id)
        for task in dag.tasks
        for upstream in task.upstream_list
    }

    print(actual_dependencies)
    assert actual_dependencies >= EXPECTED_DEPENDENCIES

def test_finish_pipeline_run_has_all_done_trigger_rule() -> None:
    task = dag.get_task('finish_pipeline_run')

    assert task.trigger_rule == 'all_done'

def test_dag_configuration() -> None:
    assert dag.dag_id == 'financial_data'
    assert dag.start_date.date() == date(2026, 1, 1)
    assert dag.catchup is False
    assert dag.schedule == '@daily'