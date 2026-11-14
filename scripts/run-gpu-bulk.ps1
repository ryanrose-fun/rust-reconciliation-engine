param(
    [ValidateRange(100, 2000000)][int]$Payments = 1000000,
    [string]$ReportName = 'gpu-million.json',
    [switch]$Compare,
    [switch]$Multicore,
    [switch]$Resident
)
$ErrorActionPreference = 'Stop'
if (([int][bool]$Compare + [int][bool]$Multicore + [int][bool]$Resident) -gt 1) { throw 'Choose only one benchmark mode.' }
if ([IO.Path]::GetFileName($ReportName) -ne $ReportName) { throw 'ReportName must be a file name, not a path.' }
$repo = Split-Path $PSScriptRoot -Parent
Push-Location $repo
try {
    cargo build --release --locked --manifest-path perf/gpu/Cargo.toml --target-dir perf/target/gpu
    if ($LASTEXITCODE -ne 0) { throw 'GPU prototype build failed.' }
    $exe = Join-Path $repo 'perf/target/gpu/release/reconcile-gpu-prototype.exe'
    $report = Join-Path $repo "docs/performance/$ReportName"
    if ($Compare -and -not $PSBoundParameters.ContainsKey('ReportName')) {
        $report = Join-Path $repo 'docs/performance/cpu-gpu-million.json'
    }
    if ($Multicore -and -not $PSBoundParameters.ContainsKey('ReportName')) {
        $report = Join-Path $repo 'docs/performance/multicore-million.json'
    }
    if ($Resident -and -not $PSBoundParameters.ContainsKey('ReportName')) {
        $report = Join-Path $repo 'docs/performance/gpu-resident-million.json'
    }
    if ($Resident) { & $exe $Payments $report --resident }
    elseif ($Multicore) { & $exe $Payments $report --multicore }
    elseif ($Compare) { & $exe $Payments $report --compare }
    else { & $exe $Payments $report }
    if ($LASTEXITCODE -ne 0) { throw 'GPU benchmark or correctness validation failed.' }
    $metadata = [ordered]@{
        generated_at_utc = [DateTime]::UtcNow.ToString('o')
        executable_sha256 = (Get-FileHash -LiteralPath $exe -Algorithm SHA256).Hash
        rust_source_sha256 = (Get-FileHash -LiteralPath 'perf/gpu/src/main.rs' -Algorithm SHA256).Hash
        comparison_source_sha256 = (Get-FileHash -LiteralPath 'perf/gpu/src/comparison.rs' -Algorithm SHA256).Hash
        multicore_source_sha256 = (Get-FileHash -LiteralPath 'perf/gpu/src/comparison/multicore.rs' -Algorithm SHA256).Hash
        resident_source_sha256 = (Get-FileHash -LiteralPath 'perf/gpu/src/resident.rs' -Algorithm SHA256).Hash
        resident_shader_sha256 = (Get-FileHash -LiteralPath 'perf/gpu/src/resident.wgsl' -Algorithm SHA256).Hash
        cargo_lock_sha256 = (Get-FileHash -LiteralPath 'perf/gpu/Cargo.lock' -Algorithm SHA256).Hash
        git_head = (git rev-parse HEAD)
        git_status = @(git status --short)
        rust = (rustc --version)
        gpu = @(nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv)
        cpu = @(Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors)
        cpu_benchmark_rerun = [bool]($Compare -or $Multicore)
    }
    $metadata | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath "$report.provenance.json" -Encoding UTF8
} finally { Pop-Location }
