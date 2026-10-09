from pathlib import Path
import re,json,collections
s=Path('/tmp/session-501e7b3a-7490-455e-8890-11d4a3436caf.md').read_text()
ms=list(re.finditer(r'\*\*Tool: ([^*]+)\*\*',s))
print('TOOLS',dict(collections.Counter(m.group(1) for m in ms)))
counts=collections.Counter()
for i,m in enumerate(ms):
 if m.group(1)!='sap_runQuery':continue
 b=s[m.end():ms[i+1].start() if i+1<len(ms) else len(s)]
 x=re.search(r'```json\s*(.*?)```',b,re.S)
 if not x:continue
 j=json.loads(x.group(1));out=b[x.end():]
 status='500' if '"httpStatus":500' in out else '400' if '"httpStatus":400' in out else 'success'
 counts[status]+=1
 print(status,j.get('sqlQuery','')[:190])
print('TOTAL',dict(counts))
