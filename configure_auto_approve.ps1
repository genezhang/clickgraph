# Auto-Approve Configuration Script for ClickGraph
# This script helps configure GitHub repository settings for auto-approve functionality

param(
    [switch]$ConfigureBranchProtection,
    [switch]$EnableAutoMerge,
    [switch]$TestAutoApprove,
    [string]$Branch = "main"
)

Write-Host "🤖 ClickGraph Auto-Approve Configuration" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

# Check if GitHub CLI is installed
$ghInstalled = Get-Command gh -ErrorAction SilentlyContinue
if (-not $ghInstalled) {
    Write-Host "❌ GitHub CLI (gh) is not installed. Please install it first:" -ForegroundColor Red
    Write-Host "   https://cli.github.com/" -ForegroundColor Yellow
    exit 1
}

# Check if authenticated
$authStatus = gh auth status 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Not authenticated with GitHub CLI. Please run: gh auth login" -ForegroundColor Red
    exit 1
}

Write-Host "✅ GitHub CLI authenticated" -ForegroundColor Green

if ($EnableAutoMerge) {
    Write-Host ""
    Write-Host "🔄 Enabling auto-merge for repository..." -ForegroundColor Yellow
    gh repo edit --enable-auto-merge
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Auto-merge enabled" -ForegroundColor Green
    } else {
        Write-Host "❌ Failed to enable auto-merge" -ForegroundColor Red
    }
}

if ($ConfigureBranchProtection) {
    Write-Host ""
    Write-Host "🔒 Configuring branch protection for '$Branch'..." -ForegroundColor Yellow

    # Create branch protection rule
    $protectionConfig = @{
        required_status_checks = @{
            strict = $true
            contexts = @("build", "Run tests")
        }
        required_pull_request_reviews = @{
            required_approving_review_count = 1
        }
        restrictions = $null
        enforce_admins = $true
        allow_force_pushes = $false
        allow_deletions = $false
        block_creations = $false
        required_linear_history = $false
        allow_auto_merge = $true
        delete_branch_on_merge = $true
    } | ConvertTo-Json -Depth 10

    Write-Host "Protection config: $protectionConfig" -ForegroundColor Gray

    # Note: This would require GitHub CLI extension or direct API call
    Write-Host "⚠️  Branch protection must be configured manually in GitHub web interface:" -ForegroundColor Yellow
    Write-Host "   Repository Settings → Branches → Branch protection rules" -ForegroundColor White
    Write-Host "   Add rule for branch: $Branch" -ForegroundColor White
}

if ($TestAutoApprove) {
    Write-Host ""
    Write-Host "🧪 Testing auto-approve functionality..." -ForegroundColor Yellow

    # Create a test documentation change
    $testFile = "test-auto-approve.md"
    $testContent = "# Test Auto-Approve

This is a test file to verify auto-approve functionality.
Created at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
"

    Write-Host "Creating test documentation file..." -ForegroundColor Gray
    $testContent | Out-File -FilePath $testFile -Encoding UTF8

    Write-Host "Committing and pushing test change..." -ForegroundColor Gray
    git add $testFile
    git commit -m "test: Add documentation for auto-approve testing"
    git push origin HEAD

    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Test change pushed. Create a PR to test auto-approve." -ForegroundColor Green
        Write-Host "   The auto-approve workflow should approve documentation-only changes." -ForegroundColor White
    } else {
        Write-Host "❌ Failed to push test change" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "📋 Summary of auto-approve configuration:" -ForegroundColor Cyan
Write-Host "   ✅ Dependabot: Configured for auto-merge of minor/patch updates" -ForegroundColor Green
Write-Host "   ✅ Workflows: Auto-approve workflow created" -ForegroundColor Green
Write-Host "   ⚠️  Branch Protection: Must be configured manually in GitHub web interface" -ForegroundColor Yellow
Write-Host "   ⚠️  Auto-Merge: Must be enabled in repository settings" -ForegroundColor Yellow
Write-Host ""
Write-Host "📖 See .github/BRANCH_PROTECTION.md for detailed configuration steps" -ForegroundColor White