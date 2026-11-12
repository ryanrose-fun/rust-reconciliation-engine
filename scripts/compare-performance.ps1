param([ValidateRange(5,100)][int]$Samples=15,[switch]$SkipBuild)
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
. (Join-Path $PSScriptRoot "performance-common.ps1")
Push-Location $root
try {
 if (!$SkipBuild) {
  cargo build --release --locked --manifest-path perf/Cargo.toml --target-dir target/perf
  if ($LASTEXITCODE -ne 0) {throw 'Build failed'}
 }
 New-Item docs/performance -ItemType Directory -Force | Out-Null
 $raw=Join-Path $root 'target/perf/comparison.raw.json'
 $err=Join-Path $root 'target/perf/comparison.stderr.log'
 $p=Start-Process (Join-Path $root 'target/perf/release/compare.exe') -ArgumentList "$Samples 1000 10000 100000" -WindowStyle Hidden -PassThru -RedirectStandardOutput $raw -RedirectStandardError $err
 $processHandle=$p.Handle
 $peak=0L
 while (!$p.HasExited) {$p.Refresh();$peak=[Math]::Max($peak,$p.PeakWorkingSet64);Start-Sleep -Milliseconds 100}
 $p.WaitForExit()
 Assert-ProcessSucceeded -Process $p -Name "Comparison benchmark" -ErrorLog $err
 $report=Get-Content $raw -Raw | ConvertFrom-Json
 $report | Add-Member hardware @{
  cpu=((Get-CimInstance Win32_Processor).Name -join ', ')
  logical_processors=[Environment]::ProcessorCount
  os=[Environment]::OSVersion.VersionString
  rustc=((rustc --version)-join '')
  whole_process_peak_working_set_bytes=$peak
  memory_scope='Entire comparison process, includes fixtures and allocator retention; not per algorithm.'
 }
 [IO.File]::WriteAllText((Join-Path $root 'docs/performance/comparison.json'),($report|ConvertTo-Json -Depth 30),(New-Object Text.UTF8Encoding($false)))
 Write-Output 'Saved docs/performance/comparison.json'
} finally {Pop-Location}