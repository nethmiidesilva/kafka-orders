# PowerShell equivalent of create_topics.sh, for Windows users without bash.
#   powershell -ExecutionPolicy Bypass -File .\scripts\create_topics.ps1
#
# Auto-creation is disabled in docker-compose.yml on purpose: topics should be
# explicit, with a chosen partition count and retention.
$ErrorActionPreference = "Stop"

$Broker      = if ($env:BROKER)       { $env:BROKER }       else { "kafka:29092" }
$OrdersTopic = if ($env:ORDERS_TOPIC) { $env:ORDERS_TOPIC } else { "orders" }
$DlqTopic    = if ($env:DLQ_TOPIC)    { $env:DLQ_TOPIC }    else { "orders.DLQ" }

Write-Host "Creating topic '$OrdersTopic' (3 partitions)..."
docker exec kafka kafka-topics --bootstrap-server $Broker `
  --create --if-not-exists --topic $OrdersTopic `
  --partitions 3 --replication-factor 1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Creating topic '$DlqTopic' (1 partition, 7-day retention)..."
docker exec kafka kafka-topics --bootstrap-server $Broker `
  --create --if-not-exists --topic $DlqTopic `
  --partitions 1 --replication-factor 1 `
  --config retention.ms=604800000
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
docker exec kafka kafka-topics --bootstrap-server $Broker --list
