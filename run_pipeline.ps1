# PowerShell script to run the full data processing pipeline

# Ensure necessary directories exist locally
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
    # Handle single object vs array of objects
    if ($rawStatus -match "^\{") {
        $status = $rawStatus | ConvertFrom-Json
    } else {
        $status = $rawStatus | ConvertFrom-Json
    }
    
    $unhealthy = @($status) | Where-Object { 
        # Check health only for services that have healthchecks defined
        ($_.Health -ne $null -and $_.Health -ne "" -and $_.Health -ne "healthy") -and ($_.State -eq "running" -or $_.State -eq "restarting" -or $_.State -eq "created")
    }
    
    if (@($status).Count -eq 0) {
        Write-Host "No containers found. Please run 'docker-compose up -d' first."
        exit 1
    }

    if ($unhealthy.Count -eq 0) {
        $allHealthy = $true
        Write-Host "All services are healthy!"
    } else {
        Write-Host "Waiting for services to become healthy ($($retryCount + 1)/$maxRetries)..."
        Start-Sleep -Seconds 5
        $retryCount++
    }
}

if (-not $allHealthy) {
    Write-Host "Error: Services did not become healthy in time."
    docker compose ps
    exit 1
}

# Define Spark arguments as a PowerShell Array
$sparkArgs = @(
    "--packages", "io.delta:delta-spark_2.12:3.1.0",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

# Step 1: Extract Crypto Data
Write-Host "Running Bronze: Extracting Crypto Data..."
docker exec spark-master spark-submit --master spark://spark-master:7077 $sparkArgs /opt/spark/src/ingestion/extract_crypto_data.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Extraction failed with exit code $LASTEXITCODE"
    exit 1
}

# Step 2: Convert Bronze to Silver
Write-Host "Running Silver: Converting Bronze to Silver..."
docker exec spark-master spark-submit --master spark://spark-master:7077 $sparkArgs /opt/spark/src/processing/bronze_to_silver_crypto.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Bronze to Silver conversion failed with exit code $LASTEXITCODE"
    exit 1
}

# Step 3: Convert Silver to Gold and generate CSV
Write-Host "Running Gold: Converting Silver to Gold..."
docker exec spark-master spark-submit --master spark://spark-master:7077 $sparkArgs /opt/spark/src/processing/silver_to_gold_crypto_stats.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Silver to Gold conversion failed with exit code $LASTEXITCODE"
    exit 1
}

# Step 4: Register tables in Metastore
Write-Host "Running Metastore Registration..."
docker exec spark-master spark-submit --master spark://spark-master:7077 $sparkArgs /opt/spark/src/processing/fix_metastore_registration.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "Warning: Metastore registration failed! The pipeline will continue, but the Hive table might not be available."
}

# Check if Final_Report.csv is created
$finalReport = "./data/Final_Report.csv"
if (-Not (Test-Path $finalReport)) {
    Write-Host "Error: Final_Report.csv not found!"
    exit 1
}

Write-Host "Pipeline completed successfully! Report generated at $finalReport"
