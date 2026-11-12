function Get-TimingSummary($Values) {
 $times=@($Values|Sort-Object)
 if(!$times.Count){return $null}
 return @{count=$times.Count;mean_ms=($times|Measure-Object -Average).Average;p50_ms=$times[[Math]::Ceiling(0.5*$times.Count)-1];p95_ms=$times[[Math]::Ceiling(0.95*$times.Count)-1]}
}
function Get-ServerLogObservation([string]$Path,[int]$ExpectedRequests) {
 $allEvents=@(Get-Content -LiteralPath $Path | ForEach-Object {$_|ConvertFrom-Json})
 $events=@($allEvents | Where-Object {$_.event -eq 'request'})
 $droppedLogs=($allEvents | Where-Object {$_.event -eq 'request_logs_dropped'} | Measure-Object -Property count -Sum).Sum
 $counts=@{}
 foreach($event in $events){$key=[string]$event.status;if(!$counts.ContainsKey($key)){$counts[$key]=0};$counts[$key]++}
 $success=@($events|Where-Object status -eq 200)
 $timing=Get-TimingSummary @($success|ForEach-Object {[double]$_.elapsed_ms})
 $stages=@{}
 $profiled=@($success|Where-Object {$null -ne $_.stages})
 foreach($name in @('body_ms','queue_ms','decode_ms','reconcile_ms','encode_ms')){
  $stages[$name]=Get-TimingSummary @($profiled|ForEach-Object {[double]$_.stages.$name})
 }
 $total=($counts.Values|Measure-Object -Sum).Sum
 return @{dropped_request_logs_reported=$droppedLogs;logged_requests=$total;expected_client_requests_plus_warmups=$ExpectedRequests;client_count_consistent=($total -eq $ExpectedRequests);status_counts=$counts;successful_handler_latency=$timing;stages=$stages;log_path=$Path;scope='Server middleware logs include 3 warmups and exclude response transmission. Older binaries use integer milliseconds. A logged status does not prove client receipt.'}
}
function Assert-ProcessSucceeded($Process, [string]$Name, [string]$ErrorLog) {
 $code=$Process.ExitCode
 if($null -eq $code -or $code -ne 0){
  $detail=''
  if(Test-Path -LiteralPath $ErrorLog){$detail=[string](Get-Content -LiteralPath $ErrorLog -Raw)}
  if([string]::IsNullOrWhiteSpace($detail)){$detail='The process wrote no error output.'}
  $display=if($null -eq $code){'unavailable'}else{[string]$code}
  throw "$Name failed (exit code: $display). $detail Error log: $ErrorLog"
 }
}