param([string]$BaseUri = "http://127.0.0.1:3000")
$ErrorActionPreference = 'Stop'
$headers = @{}
if ($env:RECONCILE_API_KEY) { $headers.Authorization = "Bearer " + $env:RECONCILE_API_KEY }
$paymentId = "DEMO-" + [guid]::NewGuid().ToString("N")
function New-Event($source, $version, $status) {
    @{
        event_id = [guid]::NewGuid().ToString()
        source = $source
        version = $version
        payment = @{ id = $paymentId; currency = 'GBP'; amount_minor = 10000; status = $status }
    }
}
function Send-Event($event) {
    $body = $event | ConvertTo-Json -Depth 10
    Invoke-RestMethod -Uri "$BaseUri/v1/events" -Method Post -Headers $headers -ContentType 'application/json' -Body ([Text.Encoding]::UTF8.GetBytes($body))
}
function Check-Receipt($label, $receipt, $outcome, $status, $duplicate) {
    if ($receipt.outcome -ne $outcome -or $receipt.payment.status -ne $status -or $receipt.duplicate_event -ne $duplicate) {
        throw "$label returned an unexpected result: $($receipt | ConvertTo-Json -Depth 10)"
    }
    [pscustomobject]@{ Step = $label; Outcome = $receipt.outcome; Status = $receipt.payment.status; Duplicate = $receipt.duplicate_event }
}
$internal = New-Event 'Internal' 1 'Completed'
$steps = @()
$steps += Check-Receipt 'Internal arrives' (Send-Event $internal) 'Applied' 'AwaitingProvider' $false
$steps += Check-Receipt 'Provider pending' (Send-Event (New-Event 'Provider' 1 'Pending')) 'Applied' 'StatusMismatch' $false
$steps += Check-Receipt 'Provider completes' (Send-Event (New-Event 'Provider' 2 'Completed')) 'Applied' 'Matched' $false
$steps += Check-Receipt 'Retry first event' (Send-Event $internal) 'Applied' 'Matched' $true
$steps += Check-Receipt 'Late old update' (Send-Event (New-Event 'Provider' 1 'Failed')) 'Stale' 'Matched' $false
$steps | Format-Table -AutoSize
$status = Invoke-RestMethod -Headers $headers -Uri "$BaseUri/v1/payments/$paymentId"
if ($status.status -ne 'Matched' -or $status.provider.version -ne 2) { throw 'Stored status was incorrect' }
Write-Host "Stored payment: $paymentId"
Write-Host "Restart the API, then retrieve this same URL to check persistence:"
Write-Host "$BaseUri/v1/payments/$paymentId"
$status | ConvertTo-Json -Depth 10