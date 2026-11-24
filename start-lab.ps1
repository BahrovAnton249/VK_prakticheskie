$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-Docker {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed: docker $($DockerArgs -join ' ')" }
}

function Wait-Api {
    param([string]$Url, [switch]$Trino)
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        try {
            $result = Invoke-RestMethod -Uri $Url -TimeoutSec 3
            if (-not $Trino -or $result.starting -eq $false) { return }
        } catch {}
        Start-Sleep -Seconds 2
    }
    throw "Service did not become ready: $Url. Check docker compose logs."
}

function Post-Json {
    param([string]$Path, [string]$Json)
    Invoke-RestMethod -Method Post -Uri "http://localhost:8181/management/v1/$Path" -ContentType 'application/json' -Body $Json
}

Write-Host '1/6 Checking Docker Desktop...'
Invoke-Docker -DockerArgs @('info', '--format', '{{.ServerVersion}}')
Write-Host '2/6 Starting storage and catalog (first download can take several minutes)...'
Invoke-Docker -DockerArgs @('compose', 'up', '-d', 'minio', 'lakekeeper')
Wait-Api -Url 'http://localhost:8181/health'
Wait-Api -Url 'http://localhost:9000/minio/health/live'
Invoke-Docker -DockerArgs @('compose', 'run', '--rm', 'mc', '-c', 'mc alias set m http://minio:9000 minioadmin minioadmin && mc mb --ignore-existing m/warehouse m/bronze m/silver m/gold')

Write-Host '3/6 Initializing catalog...'
$info = Invoke-RestMethod -Uri 'http://localhost:8181/management/v1/info'
if (-not $info.bootstrapped) { Post-Json -Path 'bootstrap' -Json '{"accept-terms-of-use":true}' | Out-Null }
$existing = Invoke-RestMethod -Uri 'http://localhost:8181/management/v1/warehouse?project-id=00000000-0000-0000-0000-000000000000'
foreach ($file in @('create-warehouse.json', 'warehouses/bronze.json', 'warehouses/silver.json', 'warehouses/gold.json')) {
    $json = Get-Content -LiteralPath $file -Raw -Encoding UTF8
    $name = ($json | ConvertFrom-Json).'warehouse-name'
    if ($name -notin @($existing.warehouses.'name')) { Post-Json -Path 'warehouse' -Json $json | Out-Null }
}

Write-Host '4/6 Starting Trino...'
Invoke-Docker -DockerArgs @('compose', 'up', '-d', 'trino')
Wait-Api -Url 'http://localhost:8080/v1/info' -Trino

Write-Host '5/6 Creating lab tables (existing finished tables are preserved)...'
foreach ($task in @(
    @{ Query = "SELECT count(*) FROM lakekeeper.information_schema.tables WHERE table_schema = 'lakehouse' AND table_name = 'people'"; File = '/sql/01-demo.sql' },
    @{ Query = "SELECT count(*) FROM gold.information_schema.tables WHERE table_schema = 'lakehouse' AND table_name = 'sales_by_store'"; File = '/sql/02-medallion.sql' }
)) {
    $output = & docker compose exec -T trino trino --execute $task.Query
    if ($LASTEXITCODE -ne 0) { throw 'Cannot check existing tables.' }
    if (($output -join '').Trim().Trim('"') -eq '0') {
        Invoke-Docker -DockerArgs @('compose', 'exec', '-T', '-e', 'LANG=C.UTF-8', '-e', 'LC_ALL=C.UTF-8', 'trino', 'trino', '--file', $task.File)
    }
}
Write-Host '6/6 Verifying results...'
Invoke-Docker -DockerArgs @('compose', 'exec', '-T', 'trino', 'trino', '--file', '/sql/03-check.sql')
Write-Host 'Ready. DBeaver: localhost:8080, user trino, no password. See README.md.'
