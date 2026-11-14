param(
    [string]$DataDirectory = (Join-Path $env:LOCALAPPDATA 'RustGpuFinance'),
    [string]$BindAddress = '127.0.0.1:3000',
    [ValidateRange(1,2000000)][int]$Capacity = 1000000
)
$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($env:RECONCILE_API_KEY)) {
    throw 'Set RECONCILE_API_KEY to a private random token of at least 32 characters first. See docs/GPU_API.md.'
}
if ($env:RECONCILE_TENANTS_FILE) { throw 'GPU mode currently supports one tenant per process; remove RECONCILE_TENANTS_FILE.' }
$repo = Split-Path $PSScriptRoot -Parent
New-Item -ItemType Directory -Force -Path $DataDirectory | Out-Null
$data = (Resolve-Path -LiteralPath $DataDirectory).Path
$names = @('RECONCILE_BIND','RECONCILE_GPU_DB_PATH','RECONCILE_DB_PATH','RECONCILE_GPU_CAPACITY')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name,'Process') }
Push-Location $repo
try {
    $env:RECONCILE_BIND = $BindAddress
    $env:RECONCILE_GPU_DB_PATH = Join-Path $data 'gpu.sqlite3'
    $env:RECONCILE_DB_PATH = Join-Path $data 'events.sqlite3'
    $env:RECONCILE_GPU_CAPACITY = [string]$Capacity
    Write-Host "GPU journal: $env:RECONCILE_GPU_DB_PATH"
    Write-Host 'Startup replays committed updates before opening the listener. Ctrl+C drains admitted work.'
    cargo run --locked --features gpu --bin api
    if ($LASTEXITCODE -ne 0) { throw 'GPU API stopped with an error; retain the journal and inspect the error above.' }
} finally {
    Pop-Location
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name,$saved[$name],'Process') }
}
