param([switch]$Check)
$ErrorActionPreference = 'Stop'
$taskSecretsPath = 'C:\Users\bodim\Desktop\Projects\opencode\test-skill-ddic-2\secrets.json'
$taskPrivate = Get-Content -LiteralPath $taskSecretsPath -Raw | ConvertFrom-Json
$taskSapUri = [Uri]$taskPrivate.API_URL
if ($taskSapUri.Scheme -ne 'https') { throw 'Expected an HTTPS SAP URL' }
$taskEnvNames = @('ABAP_SAP_URL','ABAP_SAP_CLIENT','ABAP_SAP_USER','ABAP_SAP_PASSWORD','MCP_HTTP_PORT','SAP_SYSTEMS','OPENCODE_CONFIG')
$taskPrevious = @{}
foreach ($taskEnvName in $taskEnvNames) { $taskPrevious[$taskEnvName] = [Environment]::GetEnvironmentVariable($taskEnvName,'Process') }
try {
    $env:ABAP_SAP_URL = $taskSapUri.GetLeftPart([UriPartial]::Authority)
    $env:ABAP_SAP_CLIENT = '800'
    $env:ABAP_SAP_USER = $taskPrivate.SAP_USERID
    $env:ABAP_SAP_PASSWORD = $taskPrivate.SAP_PASSWORD
    if (-not $env:ABAP_SAP_USER -or -not $env:ABAP_SAP_PASSWORD) { throw 'Missing SAP credentials in private configuration' }
    [Environment]::SetEnvironmentVariable('MCP_HTTP_PORT',$null,'Process')
    [Environment]::SetEnvironmentVariable('SAP_SYSTEMS',$null,'Process')
    $env:OPENCODE_CONFIG = Join-Path $PSScriptRoot 'opencode.json'
    Push-Location -LiteralPath $PSScriptRoot
    try {
        if ($Check) { & opencode mcp list }
        else { & opencode }
        if ($LASTEXITCODE -ne 0) { throw "OpenCode exited with code $LASTEXITCODE" }
    } finally { Pop-Location }
} finally {
    foreach ($taskEnvName in $taskEnvNames) { [Environment]::SetEnvironmentVariable($taskEnvName,$taskPrevious[$taskEnvName],'Process') }
    $taskPrivate = $null
}
