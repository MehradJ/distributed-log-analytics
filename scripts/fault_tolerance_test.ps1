param(
    [string]$ProducerBaseUrl = "http://localhost:8001",
    [string]$ApiBaseUrl = "http://localhost:8002"
)

$ErrorActionPreference = "Stop"
$message = "while-processor-down-" + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()

Write-Host "1) Stop processor..."
docker compose stop processor | Out-Null

Write-Host "2) Send event while processor is down..."
$sendResponse = Invoke-RestMethod -Uri "$ProducerBaseUrl/send?message=$message" -Method Get
if ($sendResponse.status -ne "sent") {
    throw "Producer did not return status=sent"
}

Write-Host "3) Start processor..."
docker compose start processor | Out-Null

Write-Host "4) Wait and verify event is persisted..."
Start-Sleep -Seconds 4
$logsResponse = Invoke-RestMethod -Uri "$ApiBaseUrl/logs?limit=50" -Method Get
$matched = $logsResponse.logs | Where-Object { $_.message -eq $message }
if (-not $matched) {
    throw "Fault-tolerance event was not found in API logs"
}

Write-Host "Fault-tolerance test passed."
