param([string]$TargetDir = 'target/pilot')
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Push-Location $root
try {
    cargo build --release --locked --bin benchmark --target-dir $TargetDir
    if ($LASTEXITCODE -ne 0) { throw 'Benchmark build failed' }
    $out = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString() + '.json')
    $err = $out + '.err'
    $binary = Join-Path $root ($TargetDir + '/release/benchmark.exe')
    $process = Start-Process -FilePath $binary -WindowStyle Hidden -PassThru -RedirectStandardOutput $out -RedirectStandardError $err
    $peak = 0L
    while (-not $process.HasExited) {
        $process.Refresh()
        $peak = [Math]::Max($peak, $process.PeakWorkingSet64)
        Start-Sleep -Milliseconds 20
    }
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) { throw (Get-Content -LiteralPath $err -Raw) }
    $report = Get-Content -LiteralPath $out -Raw | ConvertFrom-Json
    $report | Add-Member -NotePropertyName peak_process_working_set_bytes -NotePropertyValue $peak
    $report | Add-Member -NotePropertyName memory_scope -NotePropertyValue 'Whole benchmark process high-water RSS sampled across all scenarios; includes datasets and allocator retention'
    $report | Add-Member -NotePropertyName rustc -NotePropertyValue ((rustc --version) -join '')
    $report | Add-Member -NotePropertyName cpu -NotePropertyValue ((Get-CimInstance Win32_Processor | Select-Object -ExpandProperty Name) -join ', ')
    $report | Add-Member -NotePropertyName os_description -NotePropertyValue ([Environment]::OSVersion.VersionString)
    [IO.File]::WriteAllText((Join-Path $root 'docs/benchmark-results.json'), ($report | ConvertTo-Json -Depth 12), (New-Object Text.UTF8Encoding($false)))
    Write-Output 'Saved docs/benchmark-results.json'
} finally { Pop-Location }