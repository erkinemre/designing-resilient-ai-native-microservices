param([string]$BaseUrl = 'http://localhost:8080')
$ErrorActionPreference = 'Stop'
$order = Invoke-RestMethod "$BaseUrl/orders" -Method Post -ContentType 'application/json' -Body '{"sku":"BOOK-1","quantity":2}'
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    $current = Invoke-RestMethod "$BaseUrl/orders/$($order.id)"
    if ($current.status -eq 'InventoryReserved') { break }
}
if ($current.status -ne 'InventoryReserved') { throw "Order did not complete: $($current.status)" }
$metrics = Invoke-WebRequest "$BaseUrl/metrics"
if ($metrics.Content -notmatch 'aether_processed_total [1-9]') { throw 'Processed metric was not updated.' }
$advisor = Invoke-RestMethod "$BaseUrl/advisor"
if ($advisor.recommendedReplicaFloor -lt 1) { throw 'Advisor returned an invalid floor.' }
Write-Output "PASS order=$($order.id) status=$($current.status) forecastMode=$($advisor.mode)"
