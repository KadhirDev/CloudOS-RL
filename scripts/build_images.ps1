# =============================================================================
# Build and load Docker images into Minikube
# Run once, or after any dependency change.
# =============================================================================

param(
    [switch]$ApiOnly,
    [switch]$BridgeOnly,
    [string]$Tag = "latest"
)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     CloudOS-RL  Docker Image Builder             ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Point docker CLI at Minikube's daemon so images are available to k8s directly
Write-Host "[minikube]  Pointing Docker CLI at Minikube daemon ..." -ForegroundColor Yellow
& minikube -p minikube docker-env --shell powershell | Invoke-Expression

if (-not $BridgeOnly) {
    Write-Host ""
    Write-Host "[build]  Building cloudos-api image (this takes 5-15 min first time) ..." -ForegroundColor Yellow
    docker build -f Dockerfile -t cloudos-api:$Tag .
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[build]  ❌ API image build failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "[build]  ✅ cloudos-api:$Tag built" -ForegroundColor Green
}

if (-not $ApiOnly) {
    Write-Host ""
    Write-Host "[build]  Building cloudos-bridge image ..." -ForegroundColor Yellow
    docker build -f Dockerfile.bridge -t cloudos-bridge:$Tag .
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[build]  ❌ Bridge image build failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "[build]  ✅ cloudos-bridge:$Tag built" -ForegroundColor Green
}

Write-Host ""
Write-Host "[verify]  Images available in Minikube:" -ForegroundColor Yellow
docker images | Select-String "cloudos"

Write-Host ""
Write-Host "=" * 54 -ForegroundColor Cyan
Write-Host "  Images ready. Deploy with:" -ForegroundColor Cyan
Write-Host "  .\scripts\deploy_k8s.ps1" -ForegroundColor White
Write-Host "=" * 54 -ForegroundColor Cyan