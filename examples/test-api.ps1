param([string]$BaseUri = "http://127.0.0.1:3000")
# Start the API in another terminal first: cargo run --bin api
$ErrorActionPreference = 'Stop'
$headers = @{}
if ($env:RECONCILE_API_KEY) { $headers.Authorization = "Bearer " + $env:RECONCILE_API_KEY }
$projectDir = Split-Path $PSScriptRoot -Parent
# Preserve the original JSON arrays; avoid Windows PowerShell 5.1 array wrappers.
$internalJson = [IO.File]::ReadAllText((Join-Path $projectDir 'data/internal.json'))
$providerJson = [IO.File]::ReadAllText((Join-Path $projectDir 'data/provider.json'))
$requestBody = '{"internal_payments":' + $internalJson + ',"provider_payments":' + $providerJson + '}'

Invoke-RestMethod -Headers $headers -Uri "$BaseUri/v1/reconcile" -Method Post -ContentType 'application/json' -Body ([Text.Encoding]::UTF8.GetBytes($requestBody)) | ConvertTo-Json -Depth 10