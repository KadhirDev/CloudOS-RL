# =============================================================================
# CloudOS-RL — Full Test Suite + Coverage Report (Windows PowerShell)
# =============================================================================
# Runs all tests and generates an HTML coverage report.
#
# Usage:
#   .\scripts\run_tests.ps1                  # full suite
#   .\scripts\run_tests.ps1 -Unit            # unit tests only (fast)
#   .\scripts\run_tests.ps1 -Module operator # specific module
#   .\scripts\run_tests.ps1 -Verbose         # show all test names
#   .\scripts\run_tests.ps1 -NoCoverage      # skip coverage (faster)
# =============================================================================

param(
    [switch]$Unit,          # only -m unit tests
    [switch]$Verbose,       # -v flag
    [switch]$NoCoverage,    # skip --cov flags
    [switch]$FailFast,      # stop on first failure
    [string]$Module = "",   # run tests for specific module, e.g. "operator"
    [string]$MinCoverage = "60"
)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     CloudOS-RL  Test Suite                       ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Activate venv
if   (Test-Path ".venv\Scripts\Activate.ps1") { .\.venv\Scripts\Activate.ps1 }
elseif (Test-Path ".venv\bin\activate")        { bash -c "source .venv/bin/activate" }

# Install test deps if needed
Write-Host "[deps]  Checking test dependencies ..." -ForegroundColor Yellow
python -c "import pytest, pytest_cov" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[deps]  Installing pytest + pytest-cov ..." -ForegroundColor Yellow
    pip install pytest pytest-cov --quiet
}

# Create reports directory
New-Item -ItemType Directory -Force -Path "reports" | Out-Null
New-Item -ItemType Directory -Force -Path "reports\coverage_html" | Out-Null

# Build pytest args
$PytestArgs = @("tests/")

if ($Verbose)     { $PytestArgs += "-v" }
if ($FailFast)    { $PytestArgs += "-x" }
if ($Unit)        { $PytestArgs += "-m"; $PytestArgs += "unit" }
if ($Module -ne "") {
    $PytestArgs += "tests/test_${Module}.py"
    $PytestArgs = $PytestArgs | Where-Object { $_ -ne "tests/" }
}

if (-not $NoCoverage) {
    $PytestArgs += "--cov=ai_engine"
    $PytestArgs += "--cov-report=term-missing"
    $PytestArgs += "--cov-report=html:reports/coverage_html"
    $PytestArgs += "--cov-report=json:reports/coverage.json"
    $PytestArgs += "--cov-fail-under=$MinCoverage"
}

Write-Host "[test]  Running: pytest $($PytestArgs -join ' ')" -ForegroundColor Yellow
Write-Host ""

$StartTime = Get-Date
python -m pytest @PytestArgs
$ExitCode  = $LASTEXITCODE
$Elapsed   = (Get-Date) - $StartTime

Write-Host ""
Write-Host "=" * 54 -ForegroundColor Cyan
Write-Host "  Test run completed in $([math]::Round($Elapsed.TotalSeconds, 1))s" -ForegroundColor Cyan

if ($ExitCode -eq 0) {
    Write-Host "  Result: PASSED ✅" -ForegroundColor Green
} else {
    Write-Host "  Result: FAILED ❌" -ForegroundColor Red
}

if (-not $NoCoverage -and (Test-Path "reports\coverage_html\index.html")) {
    Write-Host ""
    Write-Host "  Coverage report: reports\coverage_html\index.html" -ForegroundColor Yellow
    Write-Host "  Open:  Start-Process reports\coverage_html\index.html" -ForegroundColor White
}

Write-Host "=" * 54 -ForegroundColor Cyan
Write-Host ""

if ($ExitCode -eq 0 -and -not $NoCoverage -and (Test-Path "reports\coverage.json")) {
    $cov  = Get-Content "reports\coverage.json" | ConvertFrom-Json
    $pct  = [math]::Round($cov.totals.percent_covered, 1)
    Write-Host "  Total coverage: $pct%" -ForegroundColor $(if ($pct -ge 80) {"Green"} elseif ($pct -ge 60) {"Yellow"} else {"Red"})
    Write-Host ""
}

exit $ExitCode