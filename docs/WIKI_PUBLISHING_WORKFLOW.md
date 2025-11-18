# Wiki Publishing Workflow

Step-by-step guide for publishing ClickGraph documentation to GitHub Wiki.

## Prerequisites

- [ ] ClickGraph server running with test data loaded
- [ ] GitHub Wiki enabled in repository settings
- [ ] Python 3.8+ with `requests` library

## Phase 1: Validate Documentation (In Repo)

### 1. Start ClickGraph with Test Schema

```powershell
# Set environment
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "brahmand"
$env:GRAPH_CONFIG_PATH = ".\benchmarks\schemas\social_benchmark.yaml"

# Start server
cargo run --release --bin clickgraph
```

### 2. Load Test Data

```powershell
# Use benchmark data or generate test data
python benchmarks\data\generate_benchmark_data.py
```

### 3. Run Validation Script

```powershell
# Install dependencies
pip install requests

# Validate all wiki docs
python scripts\validate_wiki_docs.py --docs-dir docs\wiki --output docs\WIKI_VALIDATION_REPORT.md

# Check report
cat docs\WIKI_VALIDATION_REPORT.md
```

### 4. Fix Issues

Review `WIKI_VALIDATION_REPORT.md` for:
- ❌ Failed queries → Fix syntax or schema mismatch
- ⏱️ Timeouts → Add LIMIT clauses
- 💥 Errors → Fix broken examples

Iterate until validation passes.

## Phase 2: Enable GitHub Wiki

### 1. Enable Wiki Feature

1. Go to: `https://github.com/genezhang/clickgraph/settings`
2. Scroll to **Features** section
3. Check **✓ Wikis**
4. Save settings

### 2. Initialize Wiki

1. Go to: `https://github.com/genezhang/clickgraph/wiki`
2. Click **Create the first page**
3. Title: `Home`
4. Content: (temporary placeholder)
5. Click **Save Page**

This creates the wiki repository.

## Phase 3: Clone Wiki Repository

```powershell
# Clone wiki as separate git repo
cd C:\Users\GenZ
git clone https://github.com/genezhang/clickgraph.wiki.git

# Verify clone
cd clickgraph.wiki
ls
```

## Phase 4: Publish Documentation (One by One)

### Publishing Priority Order

**Week 1 - Core Documentation** (Publish First):
1. Home.md ✅
2. Quick-Start-Guide.md ✅
3. Cypher-Basic-Patterns.md ✅
4. Troubleshooting-Guide.md ✅
5. Cypher-Multi-Hop-Traversals.md ✅
6. Cypher-Functions.md ✅

**Week 2 - Production Readiness** (Publish Second):
7. Docker-Deployment.md ⚠️ (mark as tested)
8. Production-Best-Practices.md ✅
9. Performance-Query-Optimization.md ✅

**Week 3 - Advanced Features** (Publish Third):
10. Schema-Configuration-Advanced.md ✅
11. Multi-Tenancy-RBAC.md ✅
12. Architecture-Internals.md ✅

**Week 4 - Kubernetes & Use Cases** (Publish with Warnings):
13. Kubernetes-Deployment.md ⚠️ (untested - add warning)
14. Use-Case-Social-Network.md ✅
15. Use-Case-Fraud-Detection.md ✅
16. Use-Case-Knowledge-Graphs.md ✅

### Publishing Script

```powershell
# Copy validated docs to wiki repo
$sourceDir = "C:\Users\GenZ\clickgraph\docs\wiki"
$wikiDir = "C:\Users\GenZ\clickgraph.wiki"

# Copy files one by one
$files = @(
    "Home.md",
    "Quick-Start-Guide.md",
    "Cypher-Basic-Patterns.md",
    "Troubleshooting-Guide.md",
    "Cypher-Multi-Hop-Traversals.md",
    "Cypher-Functions.md"
)

foreach ($file in $files) {
    Copy-Item "$sourceDir\$file" "$wikiDir\$file" -Force
    Write-Host "✅ Copied $file"
}

# Commit and push
cd $wikiDir
git add .
git commit -m "Add Week 1 core documentation (validated)"
git push origin master

Write-Host "`n🚀 Published to: https://github.com/genezhang/clickgraph/wiki"
```

### Incremental Publishing Strategy

**Batch 1 - Week 1 (Day 1)**:
- Copy 6 core files
- Commit: "Add core documentation (validated)"
- Verify on wiki: https://github.com/genezhang/clickgraph/wiki

**Batch 2 - Week 2 (Day 2)**:
- Copy 3 production files
- Commit: "Add production deployment guides"

**Batch 3 - Week 3 (Day 3)**:
- Copy 3 advanced files
- Commit: "Add advanced features documentation"

**Batch 4 - Week 4 (Day 4)**:
- Copy 4 use case + K8s files
- Commit: "Add use cases and Kubernetes deployment (⚠️ K8s untested)"

## Phase 5: Validation Workflow

### Before Each Batch

```powershell
# 1. Validate specific files
python scripts\validate_wiki_docs.py `
    --docs-dir docs\wiki `
    --output validation_batch_N.md

# 2. Review report
cat validation_batch_N.md

# 3. Fix issues if needed

# 4. Re-validate until clean

# 5. Publish batch
```

### Continuous Validation

```powershell
# Run validation on all published docs
python scripts\validate_wiki_docs.py `
    --docs-dir C:\Users\GenZ\clickgraph.wiki `
    --output wiki_validation.md
```

## Phase 6: Post-Publishing

### 1. Update Main Repo README

Add link to Wiki:

```markdown
## 📚 Documentation

Complete documentation is available on the **[ClickGraph Wiki](https://github.com/genezhang/clickgraph/wiki)**:

- **[Quick Start Guide](https://github.com/genezhang/clickgraph/wiki/Quick-Start-Guide)** - Get running in 5 minutes
- **[Cypher Patterns](https://github.com/genezhang/clickgraph/wiki/Cypher-Basic-Patterns)** - Learn graph query syntax
- **[Production Deployment](https://github.com/genezhang/clickgraph/wiki/Docker-Deployment)** - Deploy to production
- **[Use Cases](https://github.com/genezhang/clickgraph/wiki/Use-Case-Social-Network)** - Real-world examples
```

### 2. Create Wiki Sidebar

In wiki repo, create `_Sidebar.md`:

```markdown
**Getting Started**
- [Home](Home)
- [Quick Start](Quick-Start-Guide)
- [Troubleshooting](Troubleshooting-Guide)

**Cypher Language**
- [Basic Patterns](Cypher-Basic-Patterns)
- [Multi-Hop Traversals](Cypher-Multi-Hop-Traversals)
- [Functions](Cypher-Functions)

**Production**
- [Docker Deployment](Docker-Deployment)
- [Kubernetes](Kubernetes-Deployment)
- [Best Practices](Production-Best-Practices)

**Use Cases**
- [Social Networks](Use-Case-Social-Network)
- [Fraud Detection](Use-Case-Fraud-Detection)
- [Knowledge Graphs](Use-Case-Knowledge-Graphs)
```

### 3. Create Wiki Footer

In wiki repo, create `_Footer.md`:

```markdown
**ClickGraph** | [GitHub](https://github.com/genezhang/clickgraph) | [Issues](https://github.com/genezhang/clickgraph/issues) | [Releases](https://github.com/genezhang/clickgraph/releases)
```

## Automation (Optional)

### GitHub Action for Validation

Create `.github/workflows/validate-wiki-docs.yml`:

```yaml
name: Validate Wiki Documentation

on:
  push:
    paths:
      - 'docs/wiki/**'
  pull_request:
    paths:
      - 'docs/wiki/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    services:
      clickhouse:
        image: clickhouse/clickhouse-server:latest
        ports:
          - 8123:8123
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install requests
      
      - name: Start ClickGraph
        run: |
          cargo build --release
          cargo run --release --bin clickgraph &
          sleep 10
      
      - name: Validate documentation
        run: |
          python scripts/validate_wiki_docs.py \
            --docs-dir docs/wiki \
            --output validation_report.md
      
      - name: Upload report
        uses: actions/upload-artifact@v3
        with:
          name: validation-report
          path: validation_report.md
```

## Rollback Plan

If issues found after publishing:

```powershell
# 1. Clone wiki
git clone https://github.com/genezhang/clickgraph.wiki.git

# 2. Fix files
# (edit files directly)

# 3. Commit fixes
cd clickgraph.wiki
git add .
git commit -m "Fix: Correct query syntax in Use-Case-Social-Network.md"
git push

# 4. Re-validate
python scripts\validate_wiki_docs.py --docs-dir .
```

## Checklist

- [ ] Phase 1: Validation script runs successfully
- [ ] Phase 2: GitHub Wiki enabled
- [ ] Phase 3: Wiki repository cloned
- [ ] Phase 4: Week 1 docs published (6 files)
- [ ] Phase 4: Week 2 docs published (3 files)
- [ ] Phase 4: Week 3 docs published (3 files)
- [ ] Phase 4: Week 4 docs published (4 files)
- [ ] Phase 5: All docs validated
- [ ] Phase 6: README updated with wiki links
- [ ] Phase 6: Sidebar created
- [ ] Phase 6: Footer created

## Success Criteria

✅ All Cypher queries execute without errors
✅ All documentation pages render correctly on wiki
✅ Navigation (sidebar/footer) works
✅ Links between pages work
✅ README links to wiki
✅ No broken images or references
