param(
 [ValidateRange(2,600)][int]$Seconds=10,
 [ValidateRange(1,5)][int[]]$CaseIndex=(1..5),
 [ValidatePattern('^[a-zA-Z0-9._-]+[.]json$')][string]$ReportName='event-load.json',
 [string]$ApiBinary="target/perf-api/release/api.exe",
 [switch]$SkipBuild
)
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
. (Join-Path $PSScriptRoot 'performance-common.ps1')
Push-Location $root
$names=@('RECONCILE_TENANTS_FILE','RECONCILE_EVENT_QUEUE_CAPACITY','RECONCILE_BIND','RECONCILE_API_KEY','RECONCILE_DB_PATH','RECONCILE_EVENT_WAIT_MS','RECONCILE_REQUESTS_PER_SECOND','RECONCILE_CONCURRENCY','RECONCILE_TIMEOUT_MS','RECONCILE_MAX_BODY_BYTES','RECONCILE_MAX_PAYMENTS')
$saved=@{}
foreach($name in $names){$saved[$name]=[Environment]::GetEnvironmentVariable($name,'Process')}
$server=$null
$client=$null
$api=Join-Path $root $ApiBinary
$load=Join-Path $root 'target/perf/release/event-load.exe'
function Start-IsolatedServer($prefix) {
 $probe=New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback,0)
 $probe.Start();$port=$probe.LocalEndpoint.Port;$probe.Stop()
 $env:RECONCILE_BIND="127.0.0.1:$port"
 $script:base="http://127.0.0.1:$port"
 $p=Start-Process $api -WindowStyle Hidden -PassThru -RedirectStandardOutput "$prefix.out" -RedirectStandardError "$prefix.log"
 $null=$p.Handle
 for($i=0;$i -lt 50;$i++){
  $p.Refresh()
  if($p.HasExited){throw ("API startup failed: "+(Get-Content "$prefix.log" -Raw))}
  try {if((Invoke-RestMethod "$base/health" -TimeoutSec 1).status -eq 'ok'){return $p}}catch{}
  Start-Sleep -Milliseconds 100
 }
 Stop-Process -Id $p.Id -Force
 throw 'API did not become ready'
}
function Get-DatabaseBytes($db) {
 $sum=0L
 foreach($suffix in @('','-wal','-shm')) {if(Test-Path -LiteralPath ($db+$suffix)){$sum+=(Get-Item -LiteralPath ($db+$suffix)).Length}}
 return $sum
}
try {
 Remove-Item Env:RECONCILE_TENANTS_FILE -ErrorAction SilentlyContinue
 if(!$SkipBuild){
  cargo build --release --locked --bin api --target-dir target/perf-api
  if($LASTEXITCODE -ne 0){throw 'API build failed'}
  cargo build --release --locked --manifest-path perf/Cargo.toml --bin event-load --target-dir target/perf
  if($LASTEXITCODE -ne 0){throw 'Event load client build failed'}
 }
 New-Item target/perf/events,docs/performance -ItemType Directory -Force | Out-Null
 $tag=[DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss')+'-'+[guid]::NewGuid().ToString('N').Substring(0,8)
 $cases=@(@{c=1;rate=10000;profile='capacity'},@{c=4;rate=10000;profile='capacity'},@{c=8;rate=10000;profile='capacity'},@{c=16;rate=10000;profile='capacity'},@{c=8;rate=100;profile='default-rate'})
 $results=@()
 $allPassed=$true
 $apiHash=(Get-FileHash $api -Algorithm SHA256).Hash
 $loadHash=(Get-FileHash $load -Algorithm SHA256).Hash
 for($index=1;$index -le $cases.Count;$index++){
  if($CaseIndex -notcontains $index){continue}
  $case=$cases[$index-1]
  $prefix=Join-Path $root "target/perf/events/$tag-case-$index"
  $db="$prefix.sqlite3"
  $env:RECONCILE_DB_PATH=$db
  $env:RECONCILE_EVENT_WAIT_MS='60000'
  $env:RECONCILE_EVENT_QUEUE_CAPACITY='32'
  $env:RECONCILE_API_KEY=[guid]::NewGuid().ToString('N')+[guid]::NewGuid().ToString('N')
  $env:RECONCILE_REQUESTS_PER_SECOND=[string]$case.rate
  $env:RECONCILE_CONCURRENCY='8';$env:RECONCILE_TIMEOUT_MS='10000'
  $env:RECONCILE_MAX_BODY_BYTES='1048576';$env:RECONCILE_MAX_PAYMENTS='5000'
  $server=Start-IsolatedServer "$prefix.server"
  $initialBytes=Get-DatabaseBytes $db
  $peakBytes=$initialBytes
  $server.Refresh();$cpuStart=$server.TotalProcessorTime.TotalSeconds
  Write-Host "Case $index/5: $($case.profile), concurrency $($case.c), $Seconds seconds plus bounded drain"
  $client=Start-Process $load -ArgumentList "$base $($case.c) $Seconds" -WindowStyle Hidden -PassThru -RedirectStandardOutput "$prefix.json" -RedirectStandardError "$prefix.client.log"
  $null=$client.Handle
  $peakMemory=0L
  # Six calls per lifecycle, each with bounded retries; an independent host
  # deadline also catches a stalled async timer or load-client process.
  $clientDeadline=[DateTime]::UtcNow.AddSeconds($Seconds+120)
  while(!$client.HasExited){
   $client.Refresh();$server.Refresh()
   if([DateTime]::UtcNow -gt $clientDeadline){throw "Load client exceeded its wall-clock deadline; raw evidence retained at $prefix"}
   if($server.HasExited){throw "Benchmark API exited during load; see $prefix.server.log"}
   $peakMemory=[Math]::Max($peakMemory,$server.WorkingSet64)
   $peakBytes=[Math]::Max($peakBytes,(Get-DatabaseBytes $db))
   Start-Sleep -Milliseconds 50
  }
  $client.WaitForExit();$server.Refresh()
  $cpu=$server.TotalProcessorTime.TotalSeconds-$cpuStart
  $clientCode=$client.ExitCode
  if($null -eq $clientCode -or $clientCode -notin @(0,1) -or !(Test-Path "$prefix.json")){
   Assert-ProcessSucceeded -Process $client -Name 'Event load client' -ErrorLog "$prefix.client.log"
  }
  $result=Get-Content "$prefix.json" -Raw | ConvertFrom-Json
  $headers=@{Authorization='Bearer '+$env:RECONCILE_API_KEY}
  try { $metrics=(Invoke-WebRequest -UseBasicParsing -TimeoutSec 10 -Headers $headers -Uri "$base/metrics").Content } catch { $metrics="Metrics unavailable: $($_.Exception.Message)" }
  $result | Add-Member operational_metrics $metrics
  # Force-stop only the server owned by this case; inspect its committed WAL.
  Stop-Process -Id $server.Id -Force
  $server.WaitForExit();$server.Dispose();$server=$null
  $auditRaw=& $load --audit $db $result.completed_payments $result.run_id
  $auditCode=$LASTEXITCODE
  $audit=$auditRaw | ConvertFrom-Json
  if($audit.foreign_workload_payments -gt 0){Write-Warning "Database contains payments from another workload run; this sample is invalid."}
  $casePassed=($clientCode -eq 0 -and $result.correctness_passed -eq $true -and $auditCode -eq 0 -and $audit.passed -eq $true)
  if(!$casePassed){$allPassed=$false;Write-Warning "Case $index failed validation; retaining its errors and measurements."}
  $result | Add-Member load_client_exit_code $clientCode
  $result | Add-Member validation_passed $casePassed
  $server=Start-IsolatedServer "$prefix.restart"
  $headers=@{Authorization='Bearer '+$env:RECONCILE_API_KEY}
  $restored=Invoke-RestMethod -TimeoutSec 10 -Headers $headers -Uri "$base/v1/payments/$($result.sample_payment_id)"
  if($restored.status -ne 'Matched' -or $restored.provider.version -ne 2){throw 'Restarted status was incorrect'}
  Stop-Process -Id $server.Id -Force
  $server.WaitForExit();$server.Dispose();$server=$null
  $result | Add-Member profile $case.profile
  $result | Add-Member rate_limit $case.rate
  $result | Add-Member event_queue_capacity 32
  $result | Add-Member event_admission_capacity 40
  $result | Add-Member database_audit $audit
  $result | Add-Member restart_lookup_passed $true
  $result | Add-Member database_path $db
  $result | Add-Member storage @{initial_bytes=$initialBytes;peak_db_wal_shm_bytes=$peakBytes;final_db_wal_shm_bytes=(Get-DatabaseBytes $db);main_db_bytes=(Get-Item $db).Length}
  $result | Add-Member resources @{server_cpu_seconds=$cpu;peak_sampled_server_working_set_bytes=$peakMemory;sampling_ms=50}
  $results+=$result
  $report=@{all_cases_passed=$allPassed;api_binary=$ApiBinary;generated_at=[DateTime]::UtcNow.ToString('o');api_sha256=$apiHash;client_sha256=$loadHash;cpu=((Get-CimInstance Win32_Processor).Name -join ', ');logical_processors=[Environment]::ProcessorCount;scope='Local loopback, client and API on same host; fresh SQLite WAL/FULL database per case; no warmup events; closed-loop mixed lifecycle, not an open-loop arrival test';results=$results}
  [IO.File]::WriteAllText((Join-Path $root "docs/performance/$ReportName"),($report | ConvertTo-Json -Depth 30),(New-Object Text.UTF8Encoding($false)))
  Write-Host ("  {0:N0} committed events/s; {1:N2} ms successful POST p95; {2} completed payments; validation: {3}" -f $result.unique_committed_events_per_second,$result.successful_post_attempt_latency.p95_ms,$result.completed_payments,$casePassed)
 }
 Write-Host "Saved docs/performance/$ReportName"
 if(!$allPassed){throw "Some load cases failed; inspect docs/performance/$ReportName before using the results."}
} finally {
 foreach($p in @($client,$server)){if($null -ne $p){$p.Refresh();if(!$p.HasExited){Stop-Process -Id $p.Id -Force;$p.WaitForExit()};$p.Dispose()}}
 foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$saved[$name],'Process')}
 Pop-Location
}
