"""Correct native abap-adt-mcp 2.7.1 include source URL handling."""
from pathlib import Path
import sys
target=Path(sys.argv[1]).parent/'handlers'/'ObjectSourceHandlers.js'
text=target.read_text()
old=r'if (/\/includes\/[^/]+\/source\/main\/?$/i.test(url))'
new=r'if (/\/oo\/classes\/[^/]+\/includes\/[^/]+\/source\/main\/?$/i.test(url))'
fallback=r'!/\/includes\/|\/messageclass\//i.test(url)'
fixed=r'!/\/oo\/classes\/[^/]+\/includes\/|\/messageclass\//i.test(url)'
if new in text and fixed in text:
    print('Native MCP include-source fix already applied');raise SystemExit(0)
assert text.count(old)==1 and text.count(fallback)==1, 'Unexpected MCP version/layout; no changes applied'
backup=target.with_name('ObjectSourceHandlers.js.before-include-source')
if not backup.exists():backup.write_text(text)
target.write_text(text.replace(old,new).replace(fallback,fixed))
print('Native MCP include-source fix applied; backup preserved')
