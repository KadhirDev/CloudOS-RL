# =============================================================================
# CloudOS-RL — Run Operator Controller Loop (Windows PowerShell)
# =============================================================================
# Starts the operator that watches CloudWorkload CRs and drives the
# RL scheduling pipeline.
#
# Usage:
#   .\scripts\run_operator.ps1
#   .\scripts\run_operator.ps1 -DryRun
#   .\scripts\run_operator.ps1 -NoShap -PollInterval 10
#   .\scripts\run_operator.ps1 -RunOnce
#
# Prerequisites:
#   - Minikube running: minikube status
#   - CRD applied:      kubectl get crd cloudworkloads.cloudos.ai
#   - Namespace exists: kubectl get namespace cloudos-rl
# =============================================================================

param(
    [switch]$DryRun,
    [switch]$NoKafka,
    [switch]$NoShap,
    [switch]$RunOnce,
    [int]   $PollInterval = 5,
    [string]$Namespace    = "cloudos-rl",
    [string]$LogLevel     = "INFO"
)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     CloudOS-RL  Operator Controller              ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Activate venv
if   (Test-Path ".venv\Scripts\Activate.ps1") { .\.venv\Scripts\Activate.ps1 }
elseif (Test-Path ".venv\bin\activate")       { bash -c "source .venv/bin/activate" }

# Check kubectl context
Write-Host "[k8s]  Checking kubectl context ..." -ForegroundColor Yellow
$ctx = kubectl config current-context 2>$null
Write-Host "[k8s]  Context: $ctx" -ForegroundColor Green

# Check CRD exists
$crd = kubectl get crd cloudworkloads.cloudos.ai 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[k8s]  ❌ CRD not found. Run: kubectl apply -f infrastructure/k8s/crd.yaml" -ForegroundColor Red
    exit 1
}
Write-Host "[k8s]  ✅ CRD registered" -ForegroundColor Green

# Check namespace
$ns = kubectl get namespace $Namespace 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[k8s]  ❌ Namespace '$Namespace' not found. Run: kubectl apply -f infrastructure/k8s/namespace.yaml" -ForegroundColor Red
    exit 1
}
Write-Host "[k8s]  ✅ Namespace '$Namespace' exists" -ForegroundColor Green

# Build args
$Args = @("--namespace", $Namespace, "--poll-interval", $PollInterval, "--log-level", $LogLevel)
if ($DryRun)   { $Args += "--dry-run"   }
if ($NoKafka)  { $Args += "--no-kafka"  }
if ($NoShap)   { $Args += "--no-shap"   }
if ($RunOnce)  { $Args += "--run-once"  }

Write-Host ""
Write-Host "[operator]  Starting operator ..." -ForegroundColor Yellow
Write-Host "[operator]  Args: $($Args -join ' ')" -ForegroundColor Gray
Write-Host ""
Write-Host "  Submit a workload:  kubectl apply -f infrastructure/k8s/example-workload.yaml" -ForegroundColor Yellow
Write-Host "  Watch decisions:    kubectl get cloudworkloads -n $Namespace -w" -ForegroundColor Yellow
Write-Host ""

python -m ai_engine.operator @Args
