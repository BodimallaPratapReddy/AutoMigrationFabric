from app.migration.sap_tool_retries import apply_attempts, tool_attempts


def messages(count, status='error'):
    return [{'id': 'msg', 'content': [{'id': 'tool', 'state': {'metadata': {'toolCalls': [
        {'tool': 'sap-abap-adt.runQuery', 'status': status, 'input': {
            'destination': 'ECC-LOCAL', 'sqlQuery': f"SELECT REFTABLE FROM DD04L WHERE ROLLNAME = 'FIELD{i}'"}}
        for i in range(count)]}}}]}]


def test_rephrased_lookup_values_and_batched_failures_stop_at_limit():
    ledger = {'seen': [], 'failures': {}}
    events = tool_attempts(messages(12))
    assert len({e['signature'] for e in events}) == 1
    assert 'five unsuccessful MCP retries' in apply_attempts(ledger, events)
    assert len(ledger['seen']) == 6


def test_poll_duplicates_do_not_consume_retries_and_progress_resets_failures():
    ledger = {'seen': [], 'failures': {}}
    events = tool_attempts(messages(5))
    assert apply_attempts(ledger, events) is None
    assert apply_attempts(ledger, events) is None
    assert next(iter(ledger['failures'].values())) == 5
    success = {**events[0], 'id': 'success', 'status': 'completed'}
    assert apply_attempts(ledger, [success]) is None
    assert next(iter(ledger['failures'].values())) == 0

