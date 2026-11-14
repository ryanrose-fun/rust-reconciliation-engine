param([string]$BaseUrl='http://127.0.0.1:3000')
$ErrorActionPreference = 'Stop'
if (-not $env:RECONCILE_API_KEY) { throw 'Set the same RECONCILE_API_KEY used by the server.' }
$headers = @{ Authorization = "Bearer $env:RECONCILE_API_KEY" }
$payment = 'GPU-' + [Guid]::NewGuid().ToString('N')
$body = @{
    request_id = [Guid]::NewGuid().ToString()
    events = @(
        @{ event_id=[Guid]::NewGuid().ToString(); source='Internal'; version=1; payment=@{id=$payment;amount_minor=10000;currency='GBP';status='Completed'} },
        @{ event_id=[Guid]::NewGuid().ToString(); source='Provider'; version=1; payment=@{id=$payment;amount_minor=9500;currency='GBP';status='Pending'} }
    )
} | ConvertTo-Json -Depth 10
$bytes = [Text.Encoding]::UTF8.GetBytes($body)
$result = Invoke-RestMethod -Uri "$BaseUrl/v1/gpu/events" -Method Post -Headers $headers -ContentType 'application/json' -Body $bytes
$retry = Invoke-RestMethod -Uri "$BaseUrl/v1/gpu/events" -Method Post -Headers $headers -ContentType 'application/json' -Body $bytes
if (-not $retry.duplicate -or $retry.commit_sequence -ne $result.commit_sequence) { throw 'Retry validation failed' }
$result | ConvertTo-Json -Depth 12
Write-Host "Exact retry verified. Restart the API using the same data directory, then run:"
Write-Host "Invoke-RestMethod '$BaseUrl/v1/gpu/payments/$payment' -Headers @{Authorization = 'Bearer ' + `$env:RECONCILE_API_KEY} | ConvertTo-Json -Depth 10"
