param(
 [ValidateRange(2,300)][int]$Seconds=10,
 [ValidateRange(1,14)][int[]]$CaseIndex=(1..14),
 [ValidatePattern('^[a-zA-Z0-9._-]+[.]json$')][string]$ReportName='http-load.json',
 [string]$ApiBinary="target/perf-api/release/api.exe",
 [switch]$SkipBuild
)
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
. (Join-Path $PSScriptRoot "performance-common.ps1")
Push-Location $root
$names=@('RECONCILE_DB_PATH','RECONCILE_EVENT_WAIT_MS','RECONCILE_BIND','RECONCILE_API_KEY','RECONCILE_MAX_BODY_BYTES','RECONCILE_MAX_PAYMENTS','RECONCILE_CONCURRENCY','RECONCILE_TIMEOUT_MS','RECONCILE_REQUESTS_PER_SECOND')
$saved=@{}
foreach($name in $names){$saved[$name]=[Environment]::GetEnvironmentVariable($name)}
$server=$null
$client=$null
try {
 if(!$SkipBuild){
  cargo build --release --locked --bin api --target-dir target/perf-api
  if($LASTEXITCODE -ne 0){throw 'API build failed'}
  cargo build --release --locked --manifest-path perf/Cargo.toml --target-dir target/perf
  if($LASTEXITCODE -ne 0){throw 'Load tool build failed'}
 }
 New-Item docs/performance -ItemType Directory -Force | Out-Null
 New-Item target/perf/load -ItemType Directory -Force | Out-Null
 $cases=@()
 foreach($n in @(100,1000,5000)){foreach($c in @(1,4,8,16)){$cases+=@{profile='capacity';n=$n;c=$c;rps=0}}}
 $cases+=@{profile='defaults';n=1000;c=8;rps=200}
 $cases+=@{profile='capacity';n=1000;c=16;rps=250}
 $results=@()
 $runTag=[DateTime]::UtcNow.ToString("yyyyMMdd-HHmmss")+"-"+[guid]::NewGuid().ToString("N").Substring(0,8)
 $index=0
 foreach($case in $cases){
  $index++
  if($CaseIndex -notcontains $index){continue}
  # Ask Windows for a free loopback port; startup checks catch the small bind race.
  $listener=New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback,0)
  $listener.Start();$port=$listener.LocalEndpoint.Port;$listener.Stop()
  $base="http://127.0.0.1:$port"
  $env:RECONCILE_BIND="127.0.0.1:$port"
  $env:RECONCILE_API_KEY=[guid]::NewGuid().ToString('N')+[guid]::NewGuid().ToString('N')
  $env:RECONCILE_MAX_BODY_BYTES='1048576';$env:RECONCILE_MAX_PAYMENTS='5000'
  $env:RECONCILE_CONCURRENCY='8';$env:RECONCILE_TIMEOUT_MS='10000'
  $env:RECONCILE_REQUESTS_PER_SECOND=if($case.profile -eq 'defaults'){'100'}else{'10000'}
  $prefix=Join-Path $root "target/perf/load/$runTag-case-$index"
  $env:RECONCILE_DB_PATH="$prefix.events.sqlite3"
  $server=Start-Process (Join-Path $root $ApiBinary) -WindowStyle Hidden -PassThru -RedirectStandardOutput "$prefix.server.out" -RedirectStandardError "$prefix.server.log"
  $serverHandle=$server.Handle
  $ready=$false
  for($i=0;$i -lt 100;$i++){
   if($server.HasExited){Assert-ProcessSucceeded -Process $server -Name "API server" -ErrorLog "$prefix.server.log"; throw "API server exited before becoming ready. Log: $prefix.server.log"}
   try{$health=Invoke-RestMethod "$base/health";if($health.status -eq 'ok'){$ready=$true;break}}catch{}
   Start-Sleep -Milliseconds 100
  }
  if(!$ready){throw 'Isolated API did not start'}
  Write-Host "Case $index/$($cases.Count): $($case.profile), $($case.n) per source, concurrency $($case.c), arrival RPS $($case.rps)"
  $server.Refresh();$cpuStart=$server.TotalProcessorTime.TotalSeconds
  $clock=[Diagnostics.Stopwatch]::StartNew()
  $client=Start-Process (Join-Path $root 'target/perf/release/http-load.exe') -ArgumentList "$base $($case.n) $($case.c) $Seconds $($case.rps)" -WindowStyle Hidden -PassThru -RedirectStandardOutput "$prefix.json" -RedirectStandardError "$prefix.client.log"
  # Retain the native handle before Refresh; Windows PowerShell 5.1 otherwise loses ExitCode.
  $clientHandle=$client.Handle
  $serverPeak=0L;$clientPeak=0L;$clientCpu=0.0
  while(!$client.HasExited){
   $server.Refresh();$client.Refresh()
   $serverPeak=[Math]::Max($serverPeak,$server.WorkingSet64)
   $clientPeak=[Math]::Max($clientPeak,$client.WorkingSet64)
   $clientCpu=[Math]::Max($clientCpu,$client.TotalProcessorTime.TotalSeconds)
   Start-Sleep -Milliseconds 50
  }
  $client.WaitForExit();$clock.Stop();$server.Refresh()
  Assert-ProcessSucceeded -Process $client -Name "Load client" -ErrorLog "$prefix.client.log"
  $result=Get-Content "$prefix.json" -Raw | ConvertFrom-Json
  if($result.submitted_requests -ne (($result.status_counts.PSObject.Properties.Value|Measure-Object -Sum).Sum)){throw 'Status count inconsistency'}
  if($result.scheduled_requests -ne ($result.submitted_requests+$result.client_admission_drops)){throw 'Arrival count inconsistency'}
  $result|Add-Member profile $case.profile
  $result|Add-Member resource_observation @{
   seconds=$clock.Elapsed.TotalSeconds
   server_cpu_seconds=($server.TotalProcessorTime.TotalSeconds-$cpuStart)
   client_cpu_seconds_sampled=$clientCpu
   server_peak_sampled_working_set_bytes=$serverPeak
   client_peak_sampled_working_set_bytes=$clientPeak
   scope='50ms samples across client lifetime, including warmup and 1.1s cooldown; server CPU interval matches observation. Client CPU is last live sample. Not per-request allocation.'
  }
  $result|Add-Member case_index $index
  $result|Add-Member server_log_observation (Get-ServerLogObservation "$prefix.server.log" ($result.submitted_requests+3))
  $results+=$result
  Stop-Process -Id $server.Id;$server.WaitForExit();$server=$null
  $report=@{api_binary=$ApiBinary;generated_at=[DateTime]::UtcNow.ToString('o');cpu=((Get-CimInstance Win32_Processor).Name -join ', ');logical_processors=[Environment]::ProcessorCount;seconds_per_case=$Seconds;server_concurrency=8;body_limit_bytes=1048576;capacity_rate_limit=10000;default_rate_limit=100;results=$results}
  [IO.File]::WriteAllText((Join-Path $root ("docs/performance/"+$ReportName)),($report|ConvertTo-Json -Depth 30),(New-Object Text.UTF8Encoding($false)))
 }
 Write-Output "Saved docs/performance/$ReportName"
} finally {
 if($client -and !$client.HasExited){Stop-Process -Id $client.Id; $client.WaitForExit()}
 if($server -and !$server.HasExited){Stop-Process -Id $server.Id}
 foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$saved[$name])}
 Pop-Location
}