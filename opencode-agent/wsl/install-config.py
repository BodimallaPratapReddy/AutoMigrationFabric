from pathlib import Path
import shutil, json, datetime
src=Path(__file__).resolve().parent
shared=src.parent/'shared'
dst=Path.home()/'.config/opencode'
dst.mkdir(parents=True,exist_ok=True)
for source,target in [('opencode.json','opencode.jsonc'),('systems.sap.json','systems.sap.json')]:
 p=dst/target
 if p.exists(): shutil.copy2(p,p.with_name(p.name+'.backup-'+datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
 if source == 'systems.sap.json':
  systems=json.loads((src/source).read_text(encoding='utf-8-sig'))
  policy=json.loads((shared/'sap-policy.json').read_text(encoding='utf-8-sig'))
  for system in systems.values(): system['policy']=policy
  p.write_text(json.dumps(systems,indent=2)+'\n')
 else: shutil.copy2(src/source,p)
 p.chmod(0o600)
skill=dst/'skills/sap-datasource-reverse-engineering-adt/SKILL.md'
skill.parent.mkdir(parents=True,exist_ok=True)
if skill.exists(): shutil.copy2(skill,skill.with_name('SKILL.md.backup-'+datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
shutil.copy2(shared/'skills/sap-datasource-reverse-engineering-adt/SKILL.md',skill)
launch=Path.home()/'start-sap-opencode.sh'
shutil.copy2(src/'start-sap-opencode.sh',launch)
launch.chmod(0o700)
print('Installed MCP config, SAP destination, updated ADT skill and launcher. Existing service.json preserved.')

