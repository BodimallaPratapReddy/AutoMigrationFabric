"""Public OpenCode actions only: no reasoning, credentials, or raw tool output."""
from datetime import datetime, timezone
from pathlib import PurePosixPath


def tool_title(name: str, inputs: dict) -> str:
    sql = str(inputs.get('sql', '')).upper()
    if sql:
        for token, title in [('ALL_TAB_COL', 'Inspect column definitions'),
                             ('ALL_CONS', 'Inspect keys and constraints'),
                             ('ALL_DEPENDENCIES', 'Check dependent objects'),
                             ('ALL_OBJECTS', 'Check source object'),
                             ('ALL_TAB_COMMENTS', 'Read database descriptions'),
                             ('COUNT(DISTINCT', 'Profile source values and numeric ranges')]:
            if token in sql:
                return title
        return 'Run source SQL query'
    if name == 'read':
        return 'Read ' + PurePosixPath(str(inputs.get('path', 'project reference'))).name
    if name == 'skill':
        return 'Load migration guidance: ' + str(inputs.get('id', inputs.get('name', 'skill')))
    if name == 'glob':
        return 'Find project files: ' + str(inputs.get('pattern', 'matching files'))
    if name == 'execute':
        return 'Run an agent tool script'
    if name == 'webfetch':
        return 'Read a web reference'
    if name.endswith('.connect'):
        return 'Connect to ' + str(inputs.get('connection_name', 'source database'))
    if 'search' in name.lower():
        return 'Search documentation and available tools'
    return name.replace('_', ' ').replace('.', ' · ')


def project_messages(messages: list) -> list[dict]:
    entries = []
    for message in reversed(messages[:500]):
        if message.get('type') != 'assistant':
            continue
        for index, part in enumerate(message.get('content', [])):
            kind = part.get('type')
            if kind not in {'tool', 'text'}:
                continue
            timestamp = (part.get('time') or message.get('time') or {}).get('created')
            if not isinstance(timestamp, (int, float)):
                continue
            base = {'time': datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat(),
                    'engine': 'OpenCode', 'kind': 'agent',
                    'id': f"agent:{message.get('id')}:{part.get('id', index)}"}
            completed = (message.get('time') or {}).get('completed')
            if isinstance(completed, (int, float)):
                base['completed_at'] = datetime.fromtimestamp(completed / 1000, timezone.utc).isoformat()
            if kind == 'text':
                text = str(part.get('text', '')).strip()
                if not text:
                    continue
                # Large structured deliveries belong in the reviewed plan, not chat bubbles.
                if message.get('finish') == 'stop':
                    entries.append({**base, 'title': 'Migration proposal prepared', 'status': 'completed',
                                    'message': 'OpenCode returned its proposal for workflow validation.'})
                else:
                    entries.append({**base, 'title': 'Assistant update', 'message': text[:4000]})
                continue
            state = part.get('state') or {}
            calls = (state.get('metadata') or {}).get('toolCalls') or [
                {'tool': part.get('name') or part.get('tool') or 'Tool',
                 'input': state.get('input'), 'status': state.get('status')}]
            for number, call in enumerate(calls):
                inputs = call.get('input') if isinstance(call.get('input'), dict) else {}
                name = str(call.get('tool', 'Tool'))[:150]
                details = {key: str(inputs[key])[:12000] for key in (
                    'sql', 'query', 'search_query', 'path', 'pattern', 'namespace',
                    'connection_name', 'table_name', 'schema_name', 'id') if key in inputs}
                entries.append({**base, 'id': base['id'] + f':{number}', 'tool': name,
                                'title': tool_title(name, inputs)[:300], 'details': details,
                                'status': str(call.get('status') or state.get('status') or 'running')[:40]})
    return sorted(entries, key=lambda item: (item['time'], item['id']))
