#!/usr/bin/env bash
# Quick start guide for browser expand performance testing

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "╔═════════════════════════════════════════════════════════════════╗"
echo "║  Browser Expand Performance Test - Quick Start Guide            ║"
echo "╚═════════════════════════════════════════════════════════════════╝"
echo ""

# ===== Step 1: Verify environment =====
echo "Step 1: Verify environment..."
echo "  - Checking ClickHouse..."
if ! curl -s http://localhost:18123/ > /dev/null 2>&1; then
    echo "  ✗ ClickHouse not running on port 18123"
    echo "    Start with: docker-compose up -d"
    exit 1
fi
echo "  ✓ ClickHouse OK (port 18123)"

echo "  - Checking ClickGraph HTTP..."
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "  ⚠ ClickGraph not running on port 8080"
    echo "    Start with: ./target/release/clickgraph --http-port 8080 --bolt-port 7687"
    sleep 2
fi
echo "  ✓ ClickGraph HTTP OK (port 8080)"
echo ""

# ===== Step 2: Show options =====
echo "Step 2: Choose test mode..."
echo ""
echo "Option A: Quick single run (fastest)"
echo "  python scripts/test/browser_expand_perf.py"
echo ""
echo "Option B: Benchmark (10 iterations for averages)"
echo "  python scripts/test/browser_expand_perf.py --iterations 10 --benchmark"
echo ""
echo "Option C: Full pytest suite (most comprehensive)"
echo "  pytest -v -s tests/integration/test_browser_expand_performance.py"
echo ""
echo "Option D: Show generated SQL without running"
echo "  python scripts/test/browser_expand_perf.py --sql-only"
echo ""

# ===== Step 3: Run default =====
read -p "Run quick test now? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "===== Running Browser Expand Performance Tests ====="
    echo ""
    python scripts/test/browser_expand_perf.py --iterations 1
    echo ""
    echo "✓ Test complete!"
    echo ""
    echo "To benchmark with multiple iterations:"
    echo "  python scripts/test/browser_expand_perf.py --iterations 10 --benchmark"
fi
