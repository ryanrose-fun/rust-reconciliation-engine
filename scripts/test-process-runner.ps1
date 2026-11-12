$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'performance-common.ps1')
$root=Split-Path $PSScriptRoot -Parent
$folder=Join-Path $root ('target/perf/process-tests/'+[guid]::NewGuid().ToString('N'))
New-Item $folder -ItemType Directory -Force|Out-Null
$shell=(Get-Process -Id $PID).Path
foreach($code in @(0,7)){
 $log=Join-Path $folder "$code.stderr"
 $out=Join-Path $folder "$code.stdout"
 $process=Start-Process $shell -ArgumentList ('-NoProfile -Command "Start-Sleep -Milliseconds 100; exit {0}"' -f $code) -WindowStyle Hidden -PassThru -RedirectStandardOutput $out -RedirectStandardError $log
 $handle=$process.Handle
 while(!$process.HasExited){$process.Refresh();Start-Sleep -Milliseconds 10}
 $process.WaitForExit()
 if($null -eq $process.ExitCode -or $process.ExitCode -ne $code){throw 'Exit code was not preserved'}
 if($code -eq 0){
  Assert-ProcessSucceeded $process 'Regression child' $log
 }else{
  $caught=$false
  try{Assert-ProcessSucceeded $process 'Regression child' $log}catch{
   $caught=$true
   if($_.Exception.Message -notmatch 'exit code: 7' -or $_.Exception.Message -notmatch 'no error output'){
    throw 'Failure diagnostic did not include exit code and empty-log explanation'
   }
  }
  if(!$caught){throw 'Nonzero exit incorrectly accepted'}
 }
}
$caught=$false
try{Assert-ProcessSucceeded ([pscustomobject]@{ExitCode=$null}) 'Unknown child' (Join-Path $folder 'missing.log')}catch{
 $caught=$true
 if($_.Exception.Message -notmatch 'unavailable'){throw 'Missing exit status was not explained'}
}
if(!$caught){throw 'Missing exit status incorrectly accepted'}
Write-Host "Process runner regression checks passed on PowerShell $($PSVersionTable.PSVersion)."