param([string]$BaseUri = "http://127.0.0.1:3000")
# Start the API in another terminal: cargo run --bin api
$ErrorActionPreference = 'Stop'
$headers = @{}
if ($env:RECONCILE_API_KEY) { $headers.Authorization = "Bearer " + $env:RECONCILE_API_KEY }
$body = '{"internal_payments":[{"id":"PAY-001","currency":"GBP","amount_minor":10000,"status":"Completed"},{"id":"PAY-002","currency":"GBP","amount_minor":5000,"status":"Completed"}],"provider_payments":[{"id":"PAY-001","currency":"GBP","amount_minor":10000,"status":"Completed"},{"id":"PAY-002","currency":"GBP","amount_minor":4500,"status":"Pending"}]}'
$response = Invoke-RestMethod -Headers $headers -Uri "$BaseUri/v1/reconcile" -Method Post -ContentType 'application/json' -Body ([Text.Encoding]::UTF8.GetBytes($body))
$response | ConvertTo-Json -Depth 10
if ($response.summary.discrepancies -ne 1 -or $response.entries[1].result -ne 'AmountAndStatusMismatch') {
    throw 'Expected one combined mismatch. Stop and restart the API to load the updated code.'
}