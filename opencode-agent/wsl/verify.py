from pathlib import Path
import json,subprocess
r=Path.home()/'.config/opencode'
c=json.loads((r/'opencode.jsonc').read_text())
s=c['mcp']['servers']['sap']
assert all(Path(x).is_file() for x in s['command'])
assert s['codemode'] is False
assert 'name: sap-datasource-reverse-engineering-adt' in (r/'skills/sap-datasource-reverse-engineering-adt/SKILL.md').read_text()
print('Verified Linux Node and MCP executable paths, direct tool exposure, and ADT skill name.')
print('Installed MCP version:',json.loads((Path.home()/'.local/share/abap-mcp/native/node_modules/abap-adt-mcp/package.json').read_text())['version'])
subprocess.run(['bash','-n',str(Path.home()/'start-sap-opencode.sh')],check=True)
print('Launcher syntax valid; live SAP test awaits credentials entered in WSL.')
