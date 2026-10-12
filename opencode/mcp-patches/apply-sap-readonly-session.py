"""Apply the bounded read-only session fix to abap-adt-mcp 2.7.1 itself."""
from pathlib import Path
import sys

target=Path(sys.argv[1])
text=target.read_text()
marker='// fabric-readonly-session-recovery-v1'
if marker in text:
    print('Native MCP read-only session fix already applied')
    raise SystemExit(0)
factory='        adtClient.stateful = abap_adt_api_1.session_types.stateful;'
restored='dest.adtClient.stateful = abap_adt_api_1.session_types.stateful;'
guard="if (name === 'logout' || (cls.kind !== 'sessionExpired' && cls.kind !== 'csrf'))"
assert text.count(factory)==1 and text.count(restored)==1 and text.count(guard)==1, 'Unexpected MCP version/layout; no changes applied'
backup=target.with_name('index.js.before-readonly-session')
if not backup.exists():backup.write_text(text)
text=text.replace(factory,'        '+marker+'\n        adtClient.stateful = sys.policy?.readOnly === true ? abap_adt_api_1.session_types.stateless : abap_adt_api_1.session_types.stateful;')
text=text.replace(restored,'dest.adtClient.stateful = dest.system.policy?.readOnly === true ? abap_adt_api_1.session_types.stateless : abap_adt_api_1.session_types.stateful;')
text=text.replace(guard,"""// Only a policy-enforced read may recover an opaque HTTP 400.
                    // Semantic SAP errors (e.g. unknown DDIC columns) never match this.
                    const opaqueReadFailure = dest.system.policy?.readOnly === true &&
                        !['login', 'logout', 'dropSession'].includes(name) && cls.kind === 'ambiguous400' &&
                        /Request failed with status code 400/.test(String(error?.message || ''));
                    if (name === 'logout' || (cls.kind !== 'sessionExpired' && cls.kind !== 'csrf' && !opaqueReadFailure))""")
target.write_text(text)
print('Native MCP read-only session fix applied; backup preserved')
