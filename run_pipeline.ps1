# ==============================================================================
# run_pipeline.ps1  -  Crypto Data Pipeline launcher
#
# Modes
# -----
#   Default (API mode)    : .\run_pipeline.ps1
#   Stress-test (generate): .\run_pipeline.ps1 -StressTest
#
# Stress-test parameters (override with env vars before calling, or edit below):
#   STRESS_TEST_COINS       - number of synthetic coins   (default 200)
#   STRESS_TEST_DAYS        - days of hourly history       (default 30)
#   SPARK_TOTAL_CORES       - cores hint for shuffle calc  (default 22)
#   SPARK_CORES_MAX         - hard cap on cluster cores    (unset = use all)
#   SPARK_EXECUTOR_MEMORY   - explicit executor memory     (default: unset)
#   SPARK_CONTAINER_MEMORY_GB - container RAM GB           (executor gets 80%)
# ==============================================================================

param(
    [switch]$StressTest,
    [int]$Coins       = 200,
    [int]$Days        = 30,
    [string]$ExecMemory = "4g",
    [int]$TotalCores  = 22
)

# Ensure local data directories exist
$dataDirs = @("./data", "./data/bronze", "./data/silver", "./data/gold")
foreach ($dir in $dataDirs) {
    if (-Not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "Created directory: $dir"
    }
}

# Wait for services to be healthy
Write-Host "Waiting for services to be healthy..."
$maxRetries = 30
$retryCount = 0
$allHealthy = $false

while (-not $allHealthy -and $retryCount -lt $maxRetries) {
    $rawStatus = docker compose ps --format json
    if ($rawStatus -match "^\{") {
        $status = $rawStatus | ConvertFrom-Json
    } else {
        $status = $rawStatus | ConvertFrom-Json
    }

    $unhealthy = @($status) | Where-Object {
        ($_.Health -ne $null -and $_.Health -ne "" -and $_.Health -ne "healthy") -and
        ($_.State -eq "running" -or $_.State -eq "restarting" -or $_.State -eq "created")
    }

    if (@($status).Count -eq 0) {
        Write-Host "No containers found. Please run 'docker-compose up -d' first."
        exit 1
    }

    if ($unhealthy.Count -eq 0) {
        $allHealthy = $true
        Write-Host "All services are healthy!"
    } else {
        Write-Host "Waiting for services ($($retryCount + 1)/$maxRetries)..."
        Start-Sleep -Seconds 5
        $retryCount++
    }
}

if (-not $allHealthy) {
    Write-Host "Error: Services did not become healthy in time."
    docker compose ps
    exit 1
}

# Delta JARs are pre-installed in /opt/spark/jars - no --packages needed.
$sparkConf = @(
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

# Build environment variable flags for docker exec
$envVars = @(
    "-e", "SPARK_TOTAL_CORES=$TotalCores",
    "-e", "SPARK_LOG_LEVEL=INFO"
)

if ($StressTest) {
    $totalRows = $Coins * $Days * 24
    Write-Host ""
    Write-Host "============================================================"
    Write-Host " STRESS-TEST MODE"
    Write-Host "   Coins    : $Coins"
    Write-Host "   Days     : $Days  (hourly -> $totalRows rows)"
    Write-Host "   Exec mem : $ExecMemory"
    Write-Host "   Cores    : $TotalCores (no cap - uses all cluster cores)"
    Write-Host "============================================================"
    Write-Host ""

    $envVars += "-e", "INGESTION_MODE=generate"
    $envVars += "-e", "STRESS_TEST_COINS=$Coins"
    $envVars += "-e", "STRESS_TEST_DAYS=$Days"
    $envVars += "-e", "SPARK_EXECUTOR_MEMORY=$ExecMemory"
    # SPARK_CORES_MAX is intentionally NOT set so Spark claims all worker cores.

    # Encode shuffle partitions explicitly so the value is visible in the Spark UI.
    $shufflePartitions = $TotalCores * 4
    $sparkConf += "--conf", "spark.sql.shuffle.partitions=$shufflePartitions"
} else {
    $topN = $env:TOP_N_COINS
    if (-not $topN) { $topN = "50" }
    Write-Host ""
    Write-Host "============================================================"
    Write-Host " API MODE  (top-$topN coins from CoinGecko)"
    Write-Host "============================================================"
    Write-Host ""
    $envVars += "-e", "INGESTION_MODE=api"
}

Write-Host "Running pipeline: Bronze -> Silver -> Gold + metastore registration..."
$cmd = @("exec") + $envVars + @(
    "spark-master", "spark-submit",
    "--master", "spark://spark-master:7077"
) + $sparkConf + @("/opt/spark/src/main_pipeline.py")

docker $cmd
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Pipeline failed with exit code $LASTEXITCODE"
    exit 1
}

# Verify output
$finalReport = "./data/Final_Report.csv"
if (-Not (Test-Path $finalReport)) {
    Write-Host "Warning: Final_Report.csv not found (check Gold layer output)."
} else {
    $rowCount = (Import-Csv $finalReport | Measure-Object).Count
    Write-Host "Pipeline completed successfully!"
    Write-Host "Final_Report.csv: $rowCount data rows at $finalReport"
}
