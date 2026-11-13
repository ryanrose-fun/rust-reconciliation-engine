param([string]$RunDirectory)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Push-Location $root
$names = @('RECONCILE_TENANTS_FILE','RECONCILE_API_KEY','RECONCILE_BIND','DEMO_API_URL','DEMO_INTERNAL_KEY','DEMO_PROVIDER_KEY','DEMO_AUDITOR_KEY','DEMO_INVESTIGATOR_KEY','DEMO_EVENTS_FILE')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$server = $null
try {
    cargo build --locked --features client --bin api --example reconciliation_lifecycle
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    if (-not $RunDirectory) { $RunDirectory = Join-Path $root ('target/lifecycle-' + [Guid]::NewGuid().ToString('N')) }
    $null = New-Item -ItemType Directory -Force -Path $RunDirectory
    $RunDirectory = (Resolve-Path -LiteralPath $RunDirectory).Path
    $configFile = Join-Path $RunDirectory 'tenants.json'
    if (-not (Test-Path -LiteralPath $configFile)) {
        $roles = @('InternalWriter','ProviderWriter','Auditor','Investigator')
        $credentials = @($roles | ForEach-Object {
            @{ id = $_; role = $_; api_key = ([Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')) }
        })
        $config = @(@{ id = 'demo'; database = (Join-Path $RunDirectory 'events.sqlite3'); credentials = $credentials })
        [IO.File]::WriteAllText($configFile, (ConvertTo-Json -InputObject $config -Depth 10))
    }
    $tenants = Get-Content -LiteralPath $configFile -Raw | ConvertFrom-Json
    foreach ($pair in @(@('InternalWriter','DEMO_INTERNAL_KEY'),@('ProviderWriter','DEMO_PROVIDER_KEY'),@('Auditor','DEMO_AUDITOR_KEY'),@('Investigator','DEMO_INVESTIGATOR_KEY'))) {
        $key = @($tenants)[0].credentials | Where-Object { $_.role -eq $pair[0] } | Select-Object -First 1 -ExpandProperty api_key
        if (-not $key) { throw ('Missing demo role: ' + $pair[0]) }
        [Environment]::SetEnvironmentVariable($pair[1], $key, 'Process')
    }
    $probe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $probe.Start()
    $port = $probe.LocalEndpoint.Port
    $probe.Stop()
    $env:RECONCILE_TENANTS_FILE = $configFile
    $env:RECONCILE_API_KEY = $null
    $env:RECONCILE_BIND = "127.0.0.1:$port"
    $env:DEMO_API_URL = "http://127.0.0.1:$port"
    $env:DEMO_EVENTS_FILE = Join-Path $RunDirectory 'events.json'
    $server = Start-Process -FilePath (Join-Path $root 'target/debug/api.exe') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $RunDirectory 'api.log') -RedirectStandardError (Join-Path $RunDirectory 'api.err.log')
    $ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        if ($server.HasExited) { throw (Get-Content (Join-Path $RunDirectory 'api.err.log') -Raw) }
        try {
            $null = Invoke-RestMethod "$env:DEMO_API_URL/health" -Headers @{ Authorization = "Bearer $env:DEMO_AUDITOR_KEY" } -TimeoutSec 1
            $ready = $true
            break
        } catch { Start-Sleep -Milliseconds 250 }
    }
    if (-not $ready) { throw 'Demo API did not become ready' }
    & (Join-Path $root 'target/debug/examples/reconciliation_lifecycle.exe')
    if ($LASTEXITCODE -ne 0) { throw 'Lifecycle demonstration failed; retain the directory to retry the same events' }
    Write-Host "Demo state and event IDs retained in $RunDirectory"
    Write-Host 'Pass -RunDirectory with this path to repeat using the same event IDs. Keep tenants.json private.'
} finally {
    if ($server -and -not $server.HasExited) { $server | Stop-Process }
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    Pop-Location
}
