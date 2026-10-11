import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from app.migration.workflow_view_replication import OpenCodeOracleViewReplicationWorkflow
from app.migration import workflow_base


@pytest.mark.parametrize('keyless', [False, True])
def test_each_table_review_profiles_correct_source_and_preserves_mapping(monkeypatch, keyless):
    monkeypatch.setattr(workflow_base.workflow, 'patched', lambda _: True)
    tables, reviews = [], []
    for owner, name in [('ONT', 'HEADER'), ('OTHER', 'ITEM')]:
        tables.append(dict(table=dict(source_schema_name=owner, source_table_name=name),
            columns=[dict(column_name='ID', fabric_data_type='STRING', is_primary_key=False),
                     dict(column_name='UPDATED', fabric_data_type='TIMESTAMP', is_primary_key=False)],
            replication=dict(primary_key_columns=None, incremental_method='FULL', write_strategy='REPLACE', ingestion_flag=False)))
        reviews.append(dict(source=f'{owner}.{name}', discovery_provider='OPENCODE',
            columns=[dict(name='ID', fabric_type='STRING'), dict(name='UPDATED', fabric_type='TIMESTAMP')],
            watermark_candidates=[dict(column_name='UPDATED', data_type='DATE', index_details=[])], warnings=[]))
    # The second table has a declared key, and must also be profiled.
    tables[1]['replication']['primary_key_columns'] = ['ID']
    bundle = dict(tables=tables, review=dict(table_reviews=reviews, warnings=[
        'Base tables use full refresh; incremental loading and delete reconciliation are not enabled.']))
    class Harness(OpenCodeOracleViewReplicationWorkflow):
        def __init__(self):
            super().__init__()
            self._input = dict(source_connection_name='Oracle Local', source_schema_name='ONT', source_object_name='VIEW', migration_approach='ORACLE_VIEW')
            self.phases, self.checks = [], []
        async def _gate(self, phase, actions):
            review = self.state['review']
            self.phases.append((review.get('source'), phase))
            if phase == 'WAITING_FOR_COLUMN_MAPPING_APPROVAL':
                return dict(action='approve_columns', actor='reviewer', columns=[dict(name=c['name'], fabric_type=c['fabric_type']) for c in review['columns']])
            if phase == 'WAITING_FOR_PRIMARY_KEY_APPROVAL':
                return dict(action='approve_primary_key', actor='reviewer', primary_key_columns=[] if keyless else ['ID'])
            if phase == 'WAITING_FOR_WATERMARK_APPROVAL':
                return dict(action='approve_watermark', actor='reviewer', watermark_column=None if keyless and review['source']=='ONT.HEADER' else 'UPDATED')
            if phase == 'WAITING_FOR_DELETE_POLICY_APPROVAL':
                return dict(action='approve_delete_policy', actor='reviewer', delete_policy={'mode':'NONE'})
            return dict(action='approve_plan', actor='reviewer')
        async def _activity(self, name, payload, **kwargs):
            assert name == 'validate_oracle_key'
            self.checks.append(payload)
            return dict(valid=True, status='CHECKED', scope='FULL_TABLE', columns=payload['columns'])
    h = Harness()
    assert asyncio.run(h._review_bundle(bundle))['action'] == 'approve_plan'
    assert [(c['input']['source_schema_name'], c['input']['source_object_name']) for c in h.checks] == (
        [('OTHER', 'ITEM')] if keyless else [('ONT', 'HEADER'), ('OTHER', 'ITEM')])
    for review in reviews:
        phases = [p for source, p in h.phases if source == review['source']]
        assert phases[0] == 'WAITING_FOR_COLUMN_MAPPING_APPROVAL'
        assert phases[-2:] == ['WAITING_FOR_WATERMARK_APPROVAL', 'WAITING_FOR_DELETE_POLICY_APPROVAL']
    assert tables[0]['replication']['incremental_method'] == ('FULL' if keyless else 'WATERMARK')
    assert tables[1]['replication']['incremental_method'] == 'WATERMARK'
    assert all(t['columns'][0]['fabric_data_type']=='STRING' and t['replication']['ingestion_flag'] is False for t in tables)
    assert h._input['source_object_name']=='VIEW' and h.state['review'] is bundle['review']
    assert not bundle['review']['warnings']


def test_view_api_accepts_base_table_replication_choices_only_in_table_review(monkeypatch):
    from app.migration import api
    from app.migration.contracts import MigrationState
    class Handle:
        async def signal(self, name, command): self.command = command
    handle = Handle()
    client = SimpleNamespace(get_workflow_handle=lambda _: handle)
    async def state(*_):
        return MigrationState(workflow_id='wf', migration_approach='ORACLE_VIEW', source_object_name='V',
            phase='WAITING_FOR_WATERMARK_APPROVAL', review=dict(view_base_table=True, primary_key=['ID'],
            key_validation_required=True, key_validation={'valid':True}, watermark_candidates=[{'column_name':'UPDATED'}]))
    monkeypatch.setattr(api, '_state', state)
    asyncio.run(api._command(client, 'wf', 'approve_watermark', actor='reviewer', watermark_column='UPDATED'))
    assert handle.command['watermark_column']=='UPDATED'

