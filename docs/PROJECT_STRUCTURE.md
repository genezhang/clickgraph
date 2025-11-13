# Project Structure

*Last Updated: November 2, 2025*

ClickGraph follows a clean, organized directory structure for maximum maintainability.

## Root Directory

**Core Configuration Files:**
- `Cargo.toml` - Rust project configuration
- `Cargo.lock` - Dependency lock file
- `docker-compose*.yaml` - Docker configurations (main, test, benchmark)
- `Dockerfile*` - Container build instructions

**Documentation:**
- `README.md` - Project overview and quick start
- `STATUS.md` - Current development status and capabilities
- `CHANGELOG.md` - Version history and changes
- `KNOWN_ISSUES.md` - Known limitations and issues
- `DEV_ENVIRONMENT_CHECKLIST.md` - Development setup guide
- `GIT_WORKFLOW.md` - Git practices and workflow
- `TESTING_GUIDE.md` - Testing documentation
- `BENCHMARKS.md` - Performance benchmarks
- `JOURNEY_RETROSPECTIVE.md` - Development journey notes
- `NEXT_STEPS.md` - Future roadmap

**Assets:**
- `architecture.png` - System architecture diagram
- `logo.svg` - Project logo
- `LICENSE` - Apache 2.0 license

## Source Code

### `brahmand/`
Main Rust codebase:
- `src/graph_catalog/` - YAML schema configuration and graph metadata
- `src/open_cypher_parser/` - Cypher query parser
- `src/query_planner/` - Query planning and optimization
- `src/clickhouse_query_generator/` - SQL generation
- `src/server/` - HTTP and Bolt protocol servers
- `src/render_plan/` - Plan rendering and execution

### `brahmand-client/`
Client library for connecting to ClickGraph

## Tests

### `tests/python/`
Python test scripts (~40 files):
- `test_*.py` - Feature tests (path variables, optional match, relationships, etc.)
- `test_*_benchmark.py` - Benchmark tests
- `debug_*.py` - Debugging scripts
- `generate_*.py` - Test data generation
- `load_*.py` - Data loading utilities
- `run_all_tests.py` - Test runner

### `tests/sql/`
SQL test files and output samples:
- `test_*.sql` - Generated test SQL
- `*_sql.txt` - SQL output samples

### `tests/cypher/`
Cypher query test files:
- `test_*.cypher` - Sample Cypher queries

### `tests/data/`
Test data files:
- `*.json` - JSON test data
- `*.csv` - CSV test data
- `*.ipynb` - Jupyter notebooks
- `*.rs` - Rust test files

## Scripts

### `scripts/setup/`
Database setup SQL scripts:
- `setup_demo_data.sql` - Demo data setup
- `setup_test_data.sql` - Test data setup
- `setup_*_benchmark_data.sql` - Benchmark data
- `create_schema.sql`, `insert_data.sql` - Schema and data scripts

### `scripts/server/`
Server management scripts:
- `start_server_*.ps1` - Server start scripts (PowerShell)
- `start_server_*.bat` - Server start scripts (Batch)
- `test_server.ps1` - Server testing
- `configure_auto_approve.ps1` - Configuration helper

### `scripts/`
Utility scripts:
- `cleanup_*.ps1` - Project cleanup scripts

## Schemas

### `schemas/demo/`
Demo YAML graph schemas:
- `social_network.yaml` - Social network graph
- `ecommerce_graph_demo.yaml` - E-commerce demo
- `social_benchmark.yaml` - Social network benchmark
- `ecommerce_benchmark.yaml` - E-commerce benchmark
- `multi_graph_benchmark.yaml` - Multi-graph benchmark

### `schemas/test/`
Test YAML schemas:
- `test_*.yaml` - Test schemas for unit tests
- `multi_rel_test.yaml` - Multiple relationship testing

## Documentation

### `docs/`
Comprehensive documentation:
- `features.md` - Feature documentation
- `api.md` - HTTP and Bolt API reference
- `configuration.md` - Configuration guide
- `getting-started.md` - Getting started guide
- Additional guides and references

### `examples/`
Example workflows:
- `quick-start.md` - 5-minute quick start
- `ecommerce-analytics.md` - Complete e-commerce demo

### `notes/`
Feature implementation notes:
- `bolt-multi-database.md` - Bolt multi-database support
- `benchmarking.md` - Benchmark analysis
- `variable-length-paths.md` - Variable-length path implementation
- `shortest-path.md` - Shortest path algorithms
- `optional-match.md` - OPTIONAL MATCH implementation
- `pagerank.md` - PageRank algorithm
- Additional feature notes

### `archive/`
Historical documentation:
- `SESSION_*.md` - Old session notes
- `INVESTIGATION_*.md` - Investigation reports
- `CLEANUP_*.md` - Cleanup summaries
- `*.log` - Debug logs

## Build Artifacts

- `target/` - Rust build output (git ignored)
- `clickhouse_data/` - ClickHouse data directory (git ignored)
- `benchmarks/results/` - Benchmark results (git ignored)

## Hidden Files

- `.git/` - Git repository
- `.github/` - GitHub workflows and templates
- `.vscode/` - VS Code settings
- `.gitignore` - Git ignore rules
- `.dockerignore` - Docker ignore rules

---

## Key Benefits of This Structure

✅ **Clean Root Directory** - Only essential config and documentation files  
✅ **Organized Tests** - Tests grouped by type (Python, SQL, Cypher, data)  
✅ **Logical Schemas** - Demo vs test schemas clearly separated  
✅ **Findable Scripts** - Setup, server, and utility scripts organized  
✅ **Archived History** - Old docs moved to archive, not deleted  
✅ **Easy Navigation** - Clear purpose for each directory  

---

**Total Files Organized:** 97 files moved in reorganization (Nov 2, 2025)
