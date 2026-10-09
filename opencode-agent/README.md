# OpenCode Agent

This is the consolidated setup for OpenCode with read-only SAP ADT MCP.

| Directory | Purpose |
|---|---|
| docker | Combined Linux container, Compose, environment template and destination generator |
| wsl | Working native WSL setup scripts and launcher templates |
| shared/skills | Single maintained ADT skill source |
| shared/patches | Single stateless SQL-preview patch, reusable for either installation path |
| shared/sap-policy.json | Read-only policy shared by Docker and WSL installation |
| tests | Query-session regression checks |
| archive | Legacy Windows setup and original duplicate copies, retained for recovery |

## Docker

```powershell
cd C:\Users\bodim\Desktop\Projects\fabric\opencode-agent\docker
# Merge DEV/QTY/PRD values from .env.example into your .env; do not overwrite existing credentials.
docker compose build
docker compose run --rm opencode
```

The Docker build uses the parent directory so it can copy shared assets. Configure only complete DEV/QTY/PRD destinations. See docker/README.md. Default workspace remains the Windows test-skill-ddic-6 directory.

## WSL

The existing installed WSL runtime and ~/start-sap-opencode.sh are untouched and remain usable. Source setup scripts now live in wsl. To deliberately reinstall configuration and skill:

```bash
python3 /mnt/c/Users/bodim/Desktop/Projects/fabric/opencode-agent/wsl/install-config.py
python3 /mnt/c/Users/bodim/Desktop/Projects/fabric/opencode-agent/shared/patches/patch-preview-session.py
```

Installation backs up existing configuration. Docker uses DEV/QTY/PRD; WSL retains its working S4-DEMO destination template. Both install the shared SAP policy. Existing installed WSL configuration changes only when you explicitly run the installer.

## Migration

Former abap-mcp-readonly-v2 is now archive/windows-legacy. Former opencode-adt-docker is docker. Former wsl-sap-setup is wsl. Former skill-staging is shared/skills. Do not edit archived duplicates; edit shared sources and rebuild Docker or reinstall WSL as needed.

Old Windows launchers/configurations that referenced fabric/abap-mcp-readonly-v2 require path updates if you choose to use that legacy setup again. No Windows or WSL installed configuration was automatically changed. Analysis inputs/results under Projects/opencode were not moved.

Docker-specific image execution remains unvalidated until Docker Desktop's Linux engine is available. Compose and source-path checks are run separately; the shared query patch was previously validated against SAP in WSL.
