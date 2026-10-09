"""Run the Fabric notebook logic offline without its final execution/exit cells."""
import ast
from contextlib import contextmanager
from pathlib import Path
import types

import pytest


def load_notebook():
    path = Path(__file__).resolve().parents[1] / 'thirdparty/fabric/nb_migration_provisioning.py'
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    body = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == '_mode' for t in node.targets):
            break
        if isinstance(node, ast.Import) and any(a.name == 'pyodbc' for a in node.names):
            continue
        body.append(node)
    module = types.ModuleType('offline_provisioning_notebook')
    # dataclasses resolve types through the defining module.
    import sys
    sys.modules[module.__name__] = module
    exec(compile(ast.Module(body=body, type_ignores=[]), str(path), 'exec'), module.__dict__)
    return module


def test_notebook_self_tests():
    result = load_notebook().run_self_tests(verbosity=0)
    assert result['ok'], result


def test_sap_namespaced_columns_are_quoted_without_relaxing_object_names():
    module = load_notebook()
    assert module.quote_ident('/DMBE/DEALNUMBER', column=True) == '`/DMBE/DEALNUMBER`'
    with pytest.raises(module.ValidationError):
        module.quote_ident('/DMBE/DEALNUMBER')
    for name in ['field` STRING); DROP TABLE t; --', 'field\nname', 'field.name']:
        with pytest.raises(module.ValidationError):
            module.quote_ident(name, column=True)


@pytest.mark.parametrize('configured_type', ['VARCHAR(3)', 'VARCHAR(6)'])
def test_sap_client_and_time_text_types_provision_as_lakehouse_strings(configured_type):
    assert load_notebook().normalize_fabric_type(configured_type) == 'STRING'


@pytest.mark.parametrize('reset', [False, True])
def test_metadata_commit_resets_only_requested_state_in_same_transaction(reset):
    module = load_notebook()
    class DB:
        in_transaction = False
        statements = []
        @contextmanager
        def transaction(self):
            self.in_transaction = True
            try:
                yield
            finally:
                self.in_transaction = False
        def fetch_one(self, *_):
            return {'ProvisioningStatus': 'PROVISIONED', 'IsActive': True}
        def execute(self, sql, params):
            assert self.in_transaction
            self.statements.append((sql, params))
            return 1
    db = DB()
    module.ConfigRepository(db).apply_metadata_change(
        'existing-guid', 'PROVISIONED', [], {}, complete_run_guid='plan-guid',
        resume_ingestion=True, reset_watermark=reset)
    statements = [sql for sql, _ in db.statements]
    resets = [sql for sql in statements if 'UPDATE bronze_replication.ReplicationState' in sql]
    assert len(resets) == int(reset)
    if reset:
        assert "LastWatermarkValue = NULL" in resets[0]
        assert "Status = 'NOT_STARTED'" in resets[0]
        assert "Status <> 'RUNNING'" in resets[0]
        assert next(i for i, sql in enumerate(statements) if 'ReplicationState' in sql) < next(
            i for i, sql in enumerate(statements) if 'SET IngestionFlag' in sql)


def test_missing_or_running_state_rolls_back_metadata_commit():
    module = load_notebook()
    class DB:
        rolled_back = False
        resumed = False
        @contextmanager
        def transaction(self):
            try:
                yield
            except Exception:
                self.rolled_back = True
                raise
        def fetch_one(self, *_):
            return {'ProvisioningStatus': 'PROVISIONED', 'IsActive': True}
        def execute(self, sql, params):
            if 'SET IngestionFlag' in sql:
                self.resumed = True
            return 0 if 'UPDATE bronze_replication.ReplicationState' in sql else 1
    db = DB()
    with pytest.raises(module.TargetStateError, match='missing or running'):
        module.ConfigRepository(db).apply_metadata_change(
            'existing-guid', 'PROVISIONED', [], {}, resume_ingestion=True, reset_watermark=True)
    assert db.rolled_back
    assert not db.resumed


def test_delete_policy_survives_metadata_changes_and_requires_a_fresh_baseline():
    import json
    module = load_notebook()
    policy = {'mode': 'RECONCILE', 'behavior': 'MARK', 'reconcile_interval_minutes': 1440,
              'reconcile_require_complete_snapshot': True, 'reconcile_require_consistent_snapshot': True}
    evidence = {'valid': True, 'columns': ['ID']}
    updates = module.replication_updates({'delete_policy': policy, 'key_validation': evidence})
    assert json.loads(updates['DeletePolicy']) == policy
    assert json.loads(updates['KeyValidation']) == evidence
    with pytest.raises(module.UnsafeChangeError, match='Delete policy'):
        module.check_watermark_stability({'DeletePolicy': None}, {'delete_policy': policy})
    module.check_watermark_stability({'DeletePolicy': json.dumps(policy)}, {'delete_policy': policy})
