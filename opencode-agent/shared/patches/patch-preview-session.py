"""Reapply the local abap-adt-mcp 2.7.0 stateless preview fix after reinstall."""
from pathlib import Path
import hashlib
import json
import sys

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / '.local/share/abap-mcp/native/node_modules/abap-adt-mcp'
if json.loads((root / 'package.json').read_text())['version'] != '2.7.0':
    raise SystemExit('Version differs: review implementation before applying patch')
target = root / 'dist/handlers/QueryHandlers.js'
original = target.read_text()
marker = '    // Local fix: isolate data previews from the stateful repository session.'
if marker in original:
    print('Stateless preview patch already installed')
    raise SystemExit(0)
updated = original
for method in ('runQuery', 'tableContents'):
    old = f'this.adtclient.{method}('
    if updated.count(old) != 1:
        raise SystemExit(f'Unexpected {method} implementation; no changes made')
    updated = updated.replace(old, f'this.previewClient().{method}(')
anchor = '    handleTableContents(args) {'
if updated.count(anchor) != 1:
    raise SystemExit('Unexpected query handler layout; no changes made')
helper = '''    // Local fix: isolate data previews from the stateful repository session.
    // ADT stateless requests get independent internal modes, avoiding accumulated
    // GENERATE SUBROUTINE POOL programs. Never mutate/drop the repository session.
    previewClient() {
        const preview = this.adtclient.statelessClone;
        if (!preview || preview === this.adtclient || preview.isStateful) {
            throw new Error('Data preview requires an independent stateless ADT client');
        }
        return preview;
    }
'''
updated = updated.replace(anchor, helper + anchor)
backup = target.with_suffix('.js.before-stateless-preview')
if backup.exists():
    if backup.read_text() != original:
        raise SystemExit('Backup differs from current original; no changes made')
else:
    backup.write_text(original)
target.write_text(updated)
print(json.dumps({'patched': str(target), 'backup': str(backup),
                  'sha256': hashlib.sha256(updated.encode()).hexdigest()}))

