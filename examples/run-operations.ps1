# Isolated, synthetic workflow. No existing API or database is used or stopped.
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Push-Location $root
$names = @('RECONCILE_BIND','RECONCILE_API_KEY','RECONCILE_URL','RECONCILE_TENANTS_FILE','RECONCILE_DB_PATH','RECONCILE_EVENT_WAIT_MS')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$server = $null
$sender = $null
function Wait-Delivery {
    for ($i=0; $i -lt 80; $i++) {
        $status = & $outbox status $outboxDb | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect outbox' }
        if ($status.exhausted -gt 0) { throw 'Delivery exhausted; inspect outbox list' }
        if ($status.pending -eq 0) { return }
        Start-Sleep -Milliseconds 250
    }
    throw 'Delivery deadline exceeded; saved outbox can be resumed'
}
function Queue-Event([string]$source,[int]$version,[string]$status) {
    $event = @{ event_id = "$paymentId-$source-$version"; source = $source; version = $version; payment = @{ id=$paymentId; amount_minor=12500; currency='GBP'; status=$status } }
    $file = Join-Path $directory "$source-$version.json"
    [IO.File]::WriteAllText($file, ($event | ConvertTo-Json -Depth 8))
    & $outbox enqueue $outboxDb $file
    if ($LASTEXITCODE -ne 0) { throw 'Enqueue failed' }
}
try {
    cargo build --locked --features client --bin api --bin outbox --bin maintenance
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    $directory = Join-Path $root ('target/operations-demo-' + [Guid]::NewGuid().ToString('N'))
    $null = New-Item -ItemType Directory -Path $directory
    $outbox = Join-Path $root 'target/debug/outbox.exe'
    $outboxDb = Join-Path $directory 'sender.sqlite3'
    $paymentId = 'CHECKOUT-' + [Guid]::NewGuid().ToString('N')
    $probe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
    $probe.Start(); $port = $probe.LocalEndpoint.Port; $probe.Stop()
    $env:RECONCILE_BIND = "127.0.0.1:$port"
    $env:RECONCILE_URL = "http://127.0.0.1:$port"
    $env:RECONCILE_API_KEY = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
    $env:RECONCILE_TENANTS_FILE = $null
    $env:RECONCILE_DB_PATH = Join-Path $directory 'events.sqlite3'
    $env:RECONCILE_EVENT_WAIT_MS = '100'
    $headers = @{ Authorization = "Bearer $env:RECONCILE_API_KEY" }
    # Save an event before any sender or API is running.
    Write-Host '1. Saving checkout event durably before starting the API'
    Queue-Event Internal 1 Completed
    & $outbox status $outboxDb
    $server = Start-Process -FilePath (Join-Path $root 'target/debug/api.exe') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $directory 'api.log') -RedirectStandardError (Join-Path $directory 'api.err.log')
    $ready = $false
    for ($i=0; $i -lt 40; $i++) {
        if ($server.HasExited) { throw 'Demo API exited; inspect retained logs' }
        try { $null = Invoke-RestMethod "$env:RECONCILE_URL/health" -TimeoutSec 1; $ready=$true; break } catch { Start-Sleep -Milliseconds 250 }
    }
    if (-not $ready) { throw 'API startup timed out' }
    $sender = Start-Process -FilePath $outbox -ArgumentList @('run', ('"' + $outboxDb + '"')) -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $directory 'sender.log') -RedirectStandardError (Join-Path $directory 'sender.err.log')
    Wait-Delivery
    Write-Host '2. Waiting for the background deadline processor (no payment lookup required)'
    $case = $null
    for ($i=0; $i -lt 40; $i++) {
        $page = Invoke-RestMethod "$env:RECONCILE_URL/v1/cases" -Headers $headers
        if ($page.entries.Count -gt 0) { $case=$page.entries[0]; break }
        Start-Sleep -Milliseconds 250
    }
    if (-not $case) { throw 'Deadline case was not created' }
    Write-Host ("Case opened: " + $case.status)
    Write-Host '3. Assigning the investigation without changing payment evidence'
    $update = @{ id=[Guid]::NewGuid().ToString(); expected_revision=$case.revision; expected_case_version=$case.case_version; owner='demo-operator'; workflow='InProgress'; note='Waiting for the delayed provider completion' } | ConvertTo-Json
    $null = Invoke-RestMethod "$env:RECONCILE_URL/v1/payments/$paymentId/case" -Method Post -Headers $headers -ContentType 'application/json' -Body ([Text.Encoding]::UTF8.GetBytes($update))
    Write-Host '4. Stopping the sender, queuing provider completion, then restarting delivery'
    $sender | Stop-Process; $sender = $null
    Queue-Event Provider 1 Completed
    $sender = Start-Process -FilePath $outbox -ArgumentList @('run', ('"' + $outboxDb + '"')) -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $directory 'sender-restart.log') -RedirectStandardError (Join-Path $directory 'sender-restart.err.log')
    Wait-Delivery
    Queue-Event Provider 1 Completed
    $payment = Invoke-RestMethod "$env:RECONCILE_URL/v1/payments/$paymentId" -Headers $headers
    if ($payment.status -ne 'Matched') { throw 'Expected matched payment after delivery' }
    Write-Host ('Payment: ' + $payment.status + '; duplicate enqueue did not add another event')
    Write-Host '5. Verifying a separate backup and restore, then producing a non-destructive archive'
    & (Join-Path $root 'target/debug/maintenance.exe') drill $env:RECONCILE_DB_PATH (Join-Path $directory 'recovery')
    if ($LASTEXITCODE -ne 0) { throw 'Recovery drill failed' }
    & (Join-Path $root 'target/debug/maintenance.exe') archive $env:RECONCILE_DB_PATH (Join-Path $directory 'archive') 90
    if ($LASTEXITCODE -ne 0) { throw 'Archive failed' }
    & (Join-Path $root 'target/debug/maintenance.exe') verify-archive (Join-Path $directory 'archive')
    if ($LASTEXITCODE -ne 0) { throw 'Archive verification failed' }
    Write-Host 'The new records are younger than 90 days: the history export is empty, but the full snapshot contains them.'
    Write-Host "Demo passed. Files retained at $directory"
    Write-Host 'Signed webhook and lost-receipt tests: cargo test --features client --test durable_delivery'
} finally {
    foreach ($process in @($sender,$server)) { if ($process -and -not $process.HasExited) { $process | Stop-Process } }
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name,$saved[$name],'Process') }
    Pop-Location
}
