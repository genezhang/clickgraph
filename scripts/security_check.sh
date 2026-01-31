#!/usr/bin/env bash
#
# Security Check Script for ClickGraph
# Runs comprehensive security scans and generates summary report
#
# Usage: ./scripts/security_check.sh [--full]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/docs/audits"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if running in full mode
FULL_MODE=false
if [[ "${1:-}" == "--full" ]]; then
    FULL_MODE=true
fi

echo -e "${BLUE}================================${NC}"
echo -e "${BLUE}ClickGraph Security Scan${NC}"
echo -e "${BLUE}================================${NC}"
echo "Date: $(date)"
echo "Mode: $([ "$FULL_MODE" = true ] && echo "FULL" || echo "QUICK")"
echo ""

cd "$PROJECT_ROOT"

# Function to print section header
section_header() {
    echo ""
    echo -e "${BLUE}>>> $1${NC}"
    echo "---"
}

# Function to check if command exists
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${YELLOW}⚠️  $1 not found. Install with: cargo install $1${NC}"
        return 1
    fi
    return 0
}

# ==============================================================================
# 1. Dependency Vulnerability Scan (cargo-audit)
# ==============================================================================
section_header "1. Dependency Vulnerabilities (cargo-audit)"

if check_command "cargo-audit"; then
    echo "Scanning dependencies for known vulnerabilities..."
    
    if cargo audit --color always 2>&1 | tee /tmp/clickgraph_audit.txt; then
        echo -e "${GREEN}✅ No critical vulnerabilities found${NC}"
    else
        echo -e "${YELLOW}⚠️  Advisories found (see details above)${NC}"
    fi
    
    # Count advisories
    VULN_COUNT=$(grep -c "^Crate:" /tmp/clickgraph_audit.txt 2>/dev/null | tr -d '\n' || echo "0")
    [ -z "$VULN_COUNT" ] && VULN_COUNT=0
    echo "Total advisories: $VULN_COUNT"
else
    echo -e "${YELLOW}⚠️  Skipping (cargo-audit not installed)${NC}"
    VULN_COUNT=0
fi

# ==============================================================================
# 2. Unsafe Code Usage
# ==============================================================================
section_header "2. Unsafe Code Analysis"

echo "Searching for unsafe code blocks..."
UNSAFE_COUNT=$(grep -r "unsafe" --include="*.rs" src/ 2>/dev/null | wc -l || echo "0")
echo "Total unsafe occurrences: $UNSAFE_COUNT"

if [ "$UNSAFE_COUNT" -gt 0 ]; then
    echo ""
    echo "Unsafe code locations:"
    grep -rn "unsafe" --include="*.rs" src/ 2>/dev/null | head -20 || true
fi

if [ "$UNSAFE_COUNT" -eq 0 ]; then
    echo -e "${GREEN}✅ No unsafe code found${NC}"
elif [ "$UNSAFE_COUNT" -lt 15 ]; then
    echo -e "${GREEN}✅ Minimal unsafe code ($UNSAFE_COUNT occurrences)${NC}"
else
    echo -e "${YELLOW}⚠️  Significant unsafe code usage ($UNSAFE_COUNT occurrences)${NC}"
fi

# ==============================================================================
# 3. Panic/Unwrap Analysis (Production Code Only)
# ==============================================================================
section_header "3. Panic/Unwrap Analysis (Excluding Tests)"

echo "Searching for panics/unwraps in production code..."
PANIC_COUNT=$(grep -r "panic!\|unwrap()\|expect(" --include="*.rs" src/ 2>/dev/null | \
              grep -v "test.rs\|tests/" | \
              grep -v "#\[cfg(test)\]" | \
              wc -l || echo "0")

echo "Panic/unwrap in production code: $PANIC_COUNT"

if [ "$PANIC_COUNT" -eq 0 ]; then
    echo -e "${GREEN}✅ No panics/unwraps in production code${NC}"
elif [ "$PANIC_COUNT" -lt 10 ]; then
    echo -e "${YELLOW}⚠️  Some panics/unwraps found (review recommended)${NC}"
else
    echo -e "${RED}❌ Significant panics/unwraps found (needs attention)${NC}"
fi

# ==============================================================================
# 4. Secret Detection
# ==============================================================================
section_header "4. Secret Detection"

echo "Searching for potential hardcoded secrets..."
SECRET_PATTERNS=(
    "password.*=.*[\"']"
    "api_key.*=.*[\"']"
    "secret.*=.*[\"']"
    "token.*=.*[\"']"
    "credentials.*=.*[\"']"
)

SECRETS_FOUND=0
for pattern in "${SECRET_PATTERNS[@]}"; do
    MATCHES=$(grep -ri "$pattern" --include="*.rs" --include="*.toml" src/ Cargo.toml 2>/dev/null | \
              grep -v "test" | \
              grep -v "example" || echo "")
    
    if [ -n "$MATCHES" ]; then
        echo -e "${YELLOW}⚠️  Potential secret pattern found: $pattern${NC}"
        echo "$MATCHES"
        SECRETS_FOUND=$((SECRETS_FOUND + 1))
    fi
done

if [ "$SECRETS_FOUND" -eq 0 ]; then
    echo -e "${GREEN}✅ No hardcoded secrets detected${NC}"
else
    echo -e "${RED}❌ Found $SECRETS_FOUND potential secret patterns${NC}"
fi

# ==============================================================================
# 5. Clippy Security Lints (Full Mode Only)
# ==============================================================================
if [ "$FULL_MODE" = true ]; then
    section_header "5. Clippy Security Analysis (Full Mode)"
    
    echo "Running Clippy with security-focused lints..."
    CLIPPY_WARNINGS=$(cargo clippy --all-targets --all-features -- \
        -W clippy::all \
        -W clippy::pedantic \
        -A clippy::module_name_repetitions \
        -A clippy::too_many_lines \
        -A clippy::missing_errors_doc \
        2>&1 | grep -c "^warning:" || echo "0")
    
    echo "Total Clippy warnings: $CLIPPY_WARNINGS"
    
    if [ "$CLIPPY_WARNINGS" -eq 0 ]; then
        echo -e "${GREEN}✅ No Clippy warnings${NC}"
    elif [ "$CLIPPY_WARNINGS" -lt 100 ]; then
        echo -e "${GREEN}✅ Acceptable Clippy warnings ($CLIPPY_WARNINGS)${NC}"
    else
        echo -e "${YELLOW}⚠️  Many Clippy warnings ($CLIPPY_WARNINGS) - consider cleanup${NC}"
    fi
fi

# ==============================================================================
# 6. Dependency Audit (Full Mode Only)
# ==============================================================================
if [ "$FULL_MODE" = true ]; then
    section_header "6. Dependency License Check"
    
    echo "Checking dependency licenses..."
    cargo tree --prefix none | head -30
    
    echo ""
    echo "Note: Install 'cargo-license' for detailed license analysis:"
    echo "  cargo install cargo-license"
fi

# ==============================================================================
# 7. OWASP Top 10 Quick Checks
# ==============================================================================
section_header "7. OWASP Quick Checks"

echo "A01 - SQL Injection:"
if grep -r "format!\|concat!" --include="*.rs" src/clickhouse_query_generator/ 2>/dev/null | \
   grep -i "select\|insert\|update" | head -5; then
    echo -e "${YELLOW}⚠️  String formatting in SQL generation (review for injection risks)${NC}"
else
    echo -e "${GREEN}✅ No obvious SQL injection risks${NC}"
fi

echo ""
echo "A02 - Authentication:"
AUTH_FILES=$(find src/ -name "*auth*" -o -name "*login*" 2>/dev/null | wc -l)
echo "Authentication-related files: $AUTH_FILES"
if [ "$AUTH_FILES" -gt 0 ]; then
    echo -e "${BLUE}ℹ️  Review auth implementation: src/server/bolt_protocol/auth.rs${NC}"
fi

echo ""
echo "A03 - Sensitive Data Exposure:"
if grep -ri "log::.*password\|println!.*password" --include="*.rs" src/ 2>/dev/null | head -5; then
    echo -e "${RED}❌ PASSWORD LOGGING DETECTED!${NC}"
else
    echo -e "${GREEN}✅ No obvious password logging${NC}"
fi

# ==============================================================================
# Summary Report
# ==============================================================================
section_header "8. Security Scan Summary"

echo ""
echo "╔════════════════════════════════════════════╗"
echo "║         SECURITY SCAN RESULTS              ║"
echo "╠════════════════════════════════════════════╣"
printf "║ Vulnerabilities: %-25s ║\n" "$VULN_COUNT advisories"
printf "║ Unsafe Code: %-29s ║\n" "$UNSAFE_COUNT occurrences"
printf "║ Panics (prod): %-27s ║\n" "$PANIC_COUNT found"
printf "║ Secret Patterns: %-24s ║\n" "$SECRETS_FOUND found"
if [ "$FULL_MODE" = true ]; then
    printf "║ Clippy Warnings: %-24s ║\n" "$CLIPPY_WARNINGS total"
fi
echo "╚════════════════════════════════════════════╝"
echo ""

# Overall assessment
TOTAL_ISSUES=$((VULN_COUNT + SECRETS_FOUND))

if [ "$TOTAL_ISSUES" -eq 0 ] && [ "$PANIC_COUNT" -lt 5 ]; then
    echo -e "${GREEN}✅ OVERALL: Security posture is GOOD${NC}"
    exit 0
elif [ "$TOTAL_ISSUES" -lt 3 ] && [ "$PANIC_COUNT" -lt 20 ]; then
    echo -e "${YELLOW}⚠️  OVERALL: Security posture is ACCEPTABLE (minor issues)${NC}"
    exit 0
else
    echo -e "${RED}❌ OVERALL: Security issues found - review recommended${NC}"
    exit 1
fi

# ==============================================================================
# Recommendations
# ==============================================================================
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Review detailed report: docs/audits/SECURITY_SCAN_JAN2026.md"
echo "2. Address high-priority issues: docs/audits/SECURITY_FIXES_ACTION_PLAN.md"
echo "3. Run full scan: ./scripts/security_check.sh --full"
echo "4. Schedule monthly security audits"
echo ""
echo "For detailed analysis, run:"
echo "  cargo audit --json > /tmp/audit_report.json"
echo "  cargo clippy --all-targets --all-features"
echo ""
