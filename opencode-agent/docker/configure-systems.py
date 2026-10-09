"""Load complete environment destinations without printing secrets."""
import copy
import json
import os
from pathlib import Path


def build_systems(template, env):
    enabled, skipped = {}, []
    for name in ('DEV', 'QTY', 'PRD'):
        keys = [f'SAP_{name}_{s}' for s in ('URL', 'CLIENT', 'USER', 'PASSWORD')]
        missing = [k for k in keys if not env.get(k)]
        if missing:
            if any(env.get(k) for k in keys):
                skipped.append((name, missing))
            continue
        client = env[f'SAP_{name}_CLIENT']
        if len(client) != 3 or not client.isascii() or not client.isdigit():
            raise ValueError(f'SAP_{name}_CLIENT must contain three digits')
        if not env[f'SAP_{name}_URL'].startswith('https://'):
            raise ValueError(f'SAP_{name}_URL must use https://')
        tls = env.get(f'SAP_{name}_INSECURE_TLS', 'false').lower()
        if tls not in ('true', 'false', '1', '0'):
            raise ValueError(f'SAP_{name}_INSECURE_TLS must be true or false')
        system = copy.deepcopy(template[name])
        system.update(client=client, insecureTls=tls in ('true', '1'))
        system.pop('default', None)
        enabled[name] = system
    if not enabled:
        raise ValueError('No complete SAP destination. Set URL, CLIENT, USER and PASSWORD for DEV, QTY or PRD.')
    default = env.get('SAP_DEFAULT_SYSTEM') or next(iter(enabled))
    if default not in enabled:
        raise ValueError('SAP_DEFAULT_SYSTEM must name an enabled destination')
    enabled[default]['default'] = True
    return enabled, skipped


if __name__ == '__main__':
    try:
        template = json.loads(Path('/opt/setup/systems.template.json').read_text(encoding='utf-8-sig'))
        policy = json.loads(Path('/opt/setup/sap-policy.json').read_text(encoding='utf-8-sig'))
        for system in template.values():
            system['policy'] = policy
        systems, skipped = build_systems(template, os.environ)
        output = Path.home() / '.config/opencode/systems.sap.json'
        output.write_text(json.dumps(systems, indent=2) + '\n')
        for name, missing in skipped:
            print(f'Skipped incomplete {name}: missing {", ".join(missing)}', flush=True)
        print('Enabled SAP destinations: ' + ', '.join(systems), flush=True)
    except ValueError as error:
        raise SystemExit(str(error))

