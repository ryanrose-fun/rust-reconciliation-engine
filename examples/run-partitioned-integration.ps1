param([ValidateRange(1,16)][int]$Partitions = 4)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Push-Location $root
$names = @('RECONCILE_TENANTS_FILE','RECONCILE_API_KEY','RECONCILE_BIND','DEMO_API_URL','DEMO_INTERNAL_KEY','DEMO_PROVIDER_KEY','DEMO_AUDITOR_KEY','DEMO_RUN_DIRECTORY','RECONCILE_EVENT_WAIT_MS')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$server = $null
try {
    cargo build --locked --features client --bin api --example partitioned_integration
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    $run = Join-Path $root ('target/partitioned-integration-' + [Guid]::NewGuid().ToString('N'))
    $null = New-Item -ItemType Directory -Path $run
    $roles = @('InternalWriter','ProviderWriter','Auditor')
    $credentials = @($roles | ForEach-Object { @{ id = $_; role = $_; api_key = ([Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')) } })
    $config = @(@{ id = 'demo'; partitions = $Partitions; database = (Join-Path $run 'events.sqlite3'); credentials = $credentials })
    $configFile = Join-Path $run 'tenants.json'
    [IO.File]::WriteAllText($configFile, (ConvertTo-Json -InputObject $config -Depth 10))
    foreach ($pair in @(@('InternalWriter','DEMO_INTERNAL_KEY'),@('ProviderWriter','DEMO_PROVIDER_KEY'),@('Auditor','DEMO_AUDITOR_KEY'))) {
        $key = $credentials | Where-Object { $_.role -eq $pair[0] } | Select-Object -First 1 -ExpandProperty api_key
        [Environment]::SetEnvironmentVariable($pair[1], $key, 'Process')
    }
    $probe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $probe.Start(); $port = $probe.LocalEndpoint.Port; $probe.Stop()
    $env:RECONCILE_TENANTS_FILE = $configFile
    $env:RECONCILE_API_KEY = $null
    $env:RECONCILE_EVENT_WAIT_MS = '30000'
    $env:RECONCILE_BIND = "127.0.0.1:$port"
    $env:DEMO_API_URL = "http://127.0.0.1:$port"
    $env:DEMO_RUN_DIRECTORY = $run
    $server = Start-Process -FilePath (Join-Path $root 'target/debug/api.exe') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $run 'api.log') -RedirectStandardError (Join-Path $run 'api.err.log')
    $ready = $false
    for ($i = 0; $i -lt 80; $i++) {
        if ($server.HasExited) { throw (Get-Content (Join-Path $run 'api.err.log') -Raw) }
        try { $null = Invoke-RestMethod "$env:DEMO_API_URL/health" -TimeoutSec 1; $ready = $true; break }
        catch { Start-Sleep -Milliseconds 250 }
    }
    if (-not $ready) { throw 'API did not become ready' }
    & (Join-Path $root 'target/debug/examples/partitioned_integration.exe')
    if ($LASTEXITCODE -ne 0) { throw "Integration stopped; evidence retained in $run" }
    Write-Host "Integration files retained in $run. Keep tenants.json private."
} finally {
    # This launcher owns the isolated demo process; production uses graceful Ctrl+C/SIGTERM.
    if ($server -and -not $server.HasExited) { $server | Stop-Process }
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    Pop-Location
}
