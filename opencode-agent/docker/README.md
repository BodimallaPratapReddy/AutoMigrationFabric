# OpenCode V2 with read-only SAP ADT MCP

OpenCode 2.0.23 and abap-adt-mcp 2.7.0 run in one Linux container. OpenCode launches MCP over stdio, as in WSL. No network port is needed for the terminal interface or MCP. Both preview tools use the tested stateless-clone patch; repository operations retain their stateful client. This fixes preview-session accumulation, not the separate function-group include-source issue.

## Start from Windows PowerShell

Start Docker Desktop in Linux-container mode. Then:

```powershell
cd C:\Users\bodim\Desktop\Projects\fabric\opencode-agent\docker
Copy-Item .env.example .env
docker compose build
docker compose run --rm opencode
```

Before launching, configure at least one destination in `.env`: `SAP_DEV_URL`, `SAP_DEV_CLIENT`, `SAP_DEV_USER` and `SAP_DEV_PASSWORD`, or the matching QTY/PRD variables. Supply passwords in the launching terminal if you prefer not to store them in `.env`. Each client must contain three digits. For example:

```powershell
$env:SAP_DEV_URL = "https://s4han2022.demo.com:8177"
$env:SAP_DEV_CLIENT = "800"
$env:SAP_DEV_USER = Read-Host "DEV username"
$env:SAP_DEV_PASSWORD = [System.Net.NetworkCredential]::new("", (Read-Host "DEV password" -AsSecureString)).Password
$env:SAP_DEV_INSECURE_TLS = "true" # Existing demo only
docker compose run --rm opencode
```

Set equivalent `SAP_QTY_*` or `SAP_PRD_*` variables for other destinations. Only complete destinations are enabled; incomplete ones are skipped with missing variable names reported. If none are complete, startup stops with a clear message. `SAP_DEFAULT_SYSTEM` optionally selects an enabled destination; otherwise the first available destination in DEV, QTY, PRD order is used. Select a destination in your prompt, for example `Use sap MCP on destination QTY`.

Configure the model provider with `/connect` inside OpenCode, or set `OPENAI_API_KEY` in the launching terminal. Provider login and sessions persist in Docker volumes, independent of WSL. This setup does not copy WSL credentials or model login files.

## Workspace and results

The default mount is `C:\Users\bodim\Desktop\Projects\opencode\test-skill-ddic-6` at `/workspace`. Files created there appear immediately in that Windows folder. The directory must already exist. Change `SAP_WORKSPACE` in `.env` for another folder. For WSL Compose use a Linux source path such as `/mnt/c/Users/bodim/Desktop/Projects/opencode/test-skill-ddic-6`.

Folder 6 currently contains inputs only. To continue an older analysis, explicitly copy its result and evidence into folder 6, or set the workspace to folder 5. No old results are copied automatically.

The ADT skill is bundled at `/home/node/.config/opencode/skills/sap-datasource-reverse-engineering-adt/SKILL.md`. Use `/mcps` to check SAP connectivity, then ask for one targeted metadata query before running the analysis. For a console check with credentials already set in the terminal:

```powershell
docker compose run --rm opencode opencode --standalone mcp list
```

## SAP configuration

`config/systems.sap.json` is the DEV/QTY/PRD template. Startup writes a filtered runtime systems file containing only complete destinations. Passwords remain environment references. Metadata/configuration preview and SELECT are enabled, with OpenCode approval prompts. Read-only enforcement and excluded toolsets match WSL. TLS verification is enabled by default. Set `SAP_DEV_INSECURE_TLS=true` only for the existing demo if needed; equivalent QTY/PRD options are separate.

Rebuild after changing bundled configuration or skill files. Exit and relaunch to use a rebuilt image. Existing Docker data volumes are preserved.

## Verification and limits

The Docker image checks the patched JavaScript syntax at build time. The same patch was validated in WSL using 45 bounded metadata queries, table preview, BAdI registry query and a subsequent repository read. Docker-specific validation is recorded separately in VALIDATION.txt. No SAP writes or SQL deployments are part of this setup.

Documentation: [OpenCode V2 local MCP](https://opencode.ai/v2/docs/mcp-servers/) and [standalone CLI](https://opencode.ai/v2/docs/cli/).

