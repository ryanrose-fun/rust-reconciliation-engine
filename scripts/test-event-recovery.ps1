param([string]$ApiBinary = (Join-Path $PSScriptRoot '../target/debug/api.exe'))
# Isolated real-process test: never uses or stops an existing API server.
$ErrorActionPreference = 'Stop'
$ApiBinary = (Resolve-Path $ApiBinary).Path
$directory = Join-Path ([IO.Path]::GetTempPath()) ('reconcile-recovery-' + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($directory) | Out-Null
$names = @('RECONCILE_TENANTS_FILE','RECONCILE_BIND','RECONCILE_DB_PATH','RECONCILE_EVENT_WAIT_MS','RECONCILE_API_KEY','RECONCILE_REQUESTS_PER_SECOND')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$listener = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback, 0)
$listener.Start()
$port = $listener.LocalEndpoint.Port
$listener.Stop()
$baseUri = 'http://127.0.0.1:' + $port
$server = $null
function Start-TestServer {
    # A fresh port also avoids Windows TIME_WAIT conflicts after a forced exit.
    $portProbe = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback, 0)
    $portProbe.Start()
    $newPort = $portProbe.LocalEndpoint.Port
    $portProbe.Stop()
    $script:baseUri = 'http://127.0.0.1:' + $newPort
    $env:RECONCILE_BIND = '127.0.0.1:' + $newPort
    $process = Start-Process -FilePath $ApiBinary -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $directory 'stdout.log') -RedirectStandardError (Join-Path $directory 'stderr.log')
    $null = $process.Handle
    for ($i = 0; $i -lt 100; $i++) {
        $process.Refresh()
        if ($process.HasExited) { throw ('Server exited: ' + (Get-Content (Join-Path $directory 'stderr.log') -Raw)) }
        try { $health = Invoke-RestMethod -Uri "$baseUri/health" -TimeoutSec 1; if ($health.status -eq 'ok') { return $process } } catch {}
        Start-Sleep -Milliseconds 50
    }
    Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
    throw 'Server did not become ready'
}
try {
    Remove-Item Env:RECONCILE_TENANTS_FILE -ErrorAction SilentlyContinue
    $env:RECONCILE_BIND = '127.0.0.1:' + $port
    $env:RECONCILE_DB_PATH = Join-Path $directory 'events.sqlite3'
    $env:RECONCILE_EVENT_WAIT_MS = '100'
    $env:RECONCILE_API_KEY = 'recovery-test-key-0000000000000000'
    $env:RECONCILE_REQUESTS_PER_SECOND = '10000'
    $headers = @{ Authorization = 'Bearer ' + $env:RECONCILE_API_KEY }
    $server = Start-TestServer
    # Raw JSON preserves the maximum u64 without a PowerShell floating-point conversion.
    $body = '{"event_id":"recovery-1","source":"Internal","version":1,"payment":{"id":"PAY-RECOVERY","currency":"GBP","amount_minor":18446744073709551615,"status":"Completed"}}'
    $receipt = Invoke-RestMethod -Headers $headers -Uri "$baseUri/v1/events" -Method Post -ContentType 'application/json' -Body $body
    if ($receipt.outcome -ne 'Applied') { throw 'Initial event was not applied' }
    Stop-Process -Id $server.Id -Force
    $server.WaitForExit()
    $server.Dispose()
    $server = $null
    Start-Sleep -Milliseconds 150
    $server = Start-TestServer
    $status = Invoke-RestMethod -Headers $headers -Uri "$baseUri/v1/payments/PAY-RECOVERY"
    if ($status.status -ne 'MissingFromProvider') { throw 'Waiting deadline did not survive restart' }
    $raw = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri "$baseUri/v1/payments/PAY-RECOVERY"
    if ($raw.Content -notmatch '"amount_minor":18446744073709551615') { throw 'Maximum amount lost precision' }
    $retry = Invoke-RestMethod -Headers $headers -Uri "$baseUri/v1/events" -Method Post -ContentType 'application/json' -Body $body
    if (!$retry.duplicate_event) { throw 'Event identity did not survive restart' }
    $provider = $body.Replace('"recovery-1"','"recovery-2"').Replace('"Internal"','"Provider"')
    $matched = Invoke-RestMethod -Headers $headers -Uri "$baseUri/v1/events" -Method Post -ContentType 'application/json' -Body $provider
    if ($matched.payment.status -ne 'Matched') { throw 'Late counterpart did not reconcile' }
    Write-Host 'PASS: forced restart preserves acknowledged event, deadline, exact amount and duplicate protection; late counterpart matches.'
} finally {
    if ($null -ne $server) {
        $server.Refresh()
        if (!$server.HasExited) { Stop-Process -Id $server.Id -Force; $server.WaitForExit() }
        $server.Dispose()
    }
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    # Delete only explicitly named files created by this test; no recursive deletion.
    foreach ($file in @('events.sqlite3','events.sqlite3-wal','events.sqlite3-shm','stdout.log','stderr.log')) {
        $path = Join-Path $directory $file
        if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path }
    }
    Remove-Item -LiteralPath $directory
}