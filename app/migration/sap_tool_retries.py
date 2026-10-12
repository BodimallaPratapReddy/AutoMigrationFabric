"""Deterministic accounting for repeated SAP MCP query-shape failures."""
import hashlib
import json
import re


def tool_attempts(messages):
    events = []
    for message in reversed(messages):
        for part_index, part in enumerate(message.get('content', [])):
            state = part.get('state', {})
            for index, call in enumerate(state.get('metadata', {}).get('toolCalls', [])):
                name = call.get('tool', '')
                status = call.get('status')
                if not name.startswith(('sap-abap-adt.', 'sap-help.', 'abap-docs.')) or status not in {'error', 'completed'}:
                    continue
                args = dict(call.get('input', {}))
                if name == 'sap-abap-adt.runQuery' and isinstance(args.get('sqlQuery'), str):
                    # Different lookup values must not restart a failing metadata query.
                    sql = re.sub(r"'(?:''|[^'])*'", "'?'", args['sqlQuery'])
                    args['sqlQuery'] = re.sub(r'\s+', ' ', sql).strip().upper()
                signature = hashlib.sha256(json.dumps([name, args], sort_keys=True).encode()).hexdigest()
                events.append({'id': f"{message.get('id')}:{part.get('id', part_index)}:{index}",
                               'signature': signature, 'tool': name, 'status': status})
    return events


def apply_attempts(ledger, events):
    for event in events:
        if event['id'] in ledger['seen']:
            continue
        ledger['seen'].append(event['id'])
        key = event['signature']
        count = ledger['failures'].get(key, 0)
        count = count + 1 if event['status'] == 'error' else 0
        ledger['failures'][key] = count
        if count >= 6:  # Initial attempt followed by five unsuccessful retries.
            return f"SAP agent stopped after five unsuccessful MCP retries for {event['tool']}. Correct the query/tool usage before restarting."
    return None
