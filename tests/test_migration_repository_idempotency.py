from unittest.mock import MagicMock, patch
from uuid import uuid4

from thirdparty.configdb.repository import (
    BatchObjectRunCreate, BatchRunCreate, ConfigDBRepository, MigrationPlanCreate,
)


def _repository_with_row(row):
    repository = ConfigDBRepository("fake")
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = row
    return repository, connection, cursor


@patch("thirdparty.configdb.utils.load_dotenv")
def test_plan_create_retry_returns_original_guid(_):
    plan = MigrationPlanCreate(
        source_connection_name="ORACLE-TEST", source_system_type="ORACLE",
        source_object_type="TABLE", source_object_name="ORDERS",
        migration_approach="ORACLE_TABLE")
    guid = uuid4()
    repository, connection, cursor = _repository_with_row((
        "ORACLE-TEST", "ORACLE", "TABLE", "ORDERS", "ORACLE_TABLE"))
    with patch.object(repository, "connect", return_value=connection):
        assert repository.create_migration_plan(plan, plan_guid=guid) == guid
    assert not any("INSERT INTO bronze_replication.MigrationPlans" in call.args[0]
                   for call in cursor.execute.call_args_list)


@patch("thirdparty.configdb.utils.load_dotenv")
def test_batch_create_retry_returns_original_guid(_):
    plan_guid, batch_id = uuid4(), uuid4()
    run = BatchRunCreate(batch_type="PROVISIONING", plan_guid=plan_guid,
                         temporal_workflow_id="workflow-1", temporal_run_id="run-1")
    repository, connection, cursor = _repository_with_row((
        "PROVISIONING", plan_guid, "workflow-1", "run-1"))
    with patch.object(repository, "connect", return_value=connection):
        assert repository.start_batch_run(run, batch_run_id=batch_id) == batch_id
    assert not any("INSERT INTO bronze_replication.BatchRuns" in call.args[0]
                   for call in cursor.execute.call_args_list)


@patch("thirdparty.configdb.utils.load_dotenv")
def test_batch_object_create_retry_returns_original_guid(_):
    batch_id, object_id, object_run_id = uuid4(), uuid4(), uuid4()
    run = BatchObjectRunCreate(batch_run_id=batch_id, object_type="TABLE",
                               object_guid=object_id, object_name="ORDERS")
    repository, connection, cursor = _repository_with_row((
        batch_id, "TABLE", object_id, "ORDERS"))
    with patch.object(repository, "connect", return_value=connection):
        assert repository.start_batch_object_run(
            run, batch_object_run_id=object_run_id) == object_run_id
    assert not any("INSERT INTO bronze_replication.BatchObjectRuns" in call.args[0]
                   for call in cursor.execute.call_args_list)
