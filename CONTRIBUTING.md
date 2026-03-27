# Contributing to ClickGraph

Thank you for your interest in contributing to ClickGraph! This document explains how to get started.

## Before You Open an Issue

Open an issue when you encounter one of the following siturations.
- A real bug that you confirmed and could not fix.
- A feature request/proposal with a design - better with rationale and use cases.
- Security vulnerabilities must follow [SECURITY.md](https://github.com/genezhang/clickgraph/blob/main/SECURITY.md) - **not** GitHub issues.

## Getting Started

### Prerequisites

- **Rust** 1.85+ (stable)
- **Docker** (for ClickHouse test instance)
- **ClickHouse** 24.8+ (via Docker or native install)

### Setup

```bash
# Clone the repository
git clone https://github.com/genezhang/clickgraph.git
cd clickgraph

# Start ClickHouse for testing
docker-compose up -d clickhouse-service

# Build
cargo build

# Run tests (all 1,600+ must pass)
cargo test

# Format and lint (required before submitting)
cargo fmt --all
cargo clippy --all-targets
```

### Running the Server

```bash
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --bin clickgraph
```

## Development Workflow

### Branch Naming

Use these prefixes:

| Prefix | Purpose |
|--------|---------|
| `fix/` | Bug fixes |
| `feature/` | New features |
| `refactor/` | Code restructuring |
| `test/` | Test additions or changes |
| `docs/` | Documentation only |
| `perf/` | Performance improvements |

### Pull Request Process

1. **Create a branch** from `main` with the appropriate prefix
2. **Make your changes** — keep PRs focused on a single concern
3. **Run the full check suite** before pushing:
   ```bash
   cargo fmt --all && cargo clippy --all-targets && cargo test
   ```
4. **Push and create a PR** against `main`
5. **All CI checks must pass** — formatting, clippy, tests, security audit
6. **PRs require review** before merging — direct pushes to `main` are blocked

### Commit Messages

Use conventional commit style:

```
fix: description of the bug fix
feature: description of the new feature
refactor: description of the restructuring
test: description of test changes
docs: description of documentation changes
perf: description of performance improvement
```

Keep the first line under 72 characters. Add a body for context when the change is non-trivial.

## Architecture Overview

```
Cypher Query -> Parse -> Plan -> Optimize -> Render -> Generate SQL -> Execute
```

| Stage | Module | Purpose |
|-------|--------|---------|
| Parse | `src/open_cypher_parser/` | Cypher to AST (nom combinators) |
| Plan | `src/query_planner/` | AST to LogicalPlan |
| Optimize | `src/query_planner/optimizer/` | Projection/filter push-down |
| Render | `src/render_plan/` | LogicalPlan to RenderPlan (CTEs, JOINs) |
| Generate | `src/clickhouse_query_generator/` | RenderPlan to ClickHouse SQL |
| Execute | `src/server/` | HTTP + Bolt servers |

### Workspace Structure

| Crate | Purpose |
|-------|---------|
| `clickgraph` | Core engine (parser, planner, SQL generator, server) |
| `clickgraph-embedded` | Embedded mode via chdb |
| `clickgraph-ffi` | UniFFI FFI layer for Go/Python bindings |
| `clickgraph-client` | CLI client |
| `clickgraph-go` | Go bindings |
| `clickgraph-py` | Python bindings |

## Ground Rules

1. **Never change query semantics** — honestly return what is asked, no more, no less
2. **No shortcuts** — fully understand the processing flow before making changes
3. **Quality over speed** — this is a late-stage project; reuse existing code before writing new
4. **Add regression tests** for every bug fix
5. **Run the full test suite** after every change to `render_plan/plan_builder_utils.rs` — most regressions originate there

## What to Work On

### Good First Issues

Look for issues labeled [`good first issue`](https://github.com/genezhang/clickgraph/labels/good%20first%20issue) on GitHub.

### Areas Where Help Is Needed

- **Issue fix verification** - verify issues are addressed properly
- **Test coverage** — especially integration tests for edge cases
- **Documentation** — schema examples, wiki pages, inline doc comments
- **Performance** — query optimization, caching improvements
- **Cypher coverage** — new Cypher features or syntax support
- **Language bindings** — improvements to Go and Python wrappers

## Testing

### Running Tests

```bash
# All Rust tests (~1,600)
cargo test

# Single test
cargo test test_name

# With output
cargo test -- --nocapture

# Integration tests (requires running ClickHouse + ClickGraph server)
pytest tests/integration/

# Go binding tests
cd clickgraph-go && CGO_LDFLAGS="-L../target/debug" LD_LIBRARY_PATH="../target/debug" go test -v

# Python binding tests
cd clickgraph-py && LD_LIBRARY_PATH="../target/debug" python3 -m pytest tests/ -v
```

### Writing Tests

- **Unit tests** go in the same file as the code (`#[cfg(test)] mod tests`)
- **Integration tests** go in `tests/rust/integration/`
- **SQL generation tests** use the `generate_sql` helper pattern from existing tests
- **Schema test data** uses schemas in `schemas/test/` or `benchmarks/`

## Key Documentation

| Document | Purpose |
|----------|---------|
| `CLAUDE.md` | AI assistant guidance and architecture details |
| `STATUS.md` | Current project state |
| `CHANGELOG.md` | Release history |
| `DEV_QUICK_START.md` | Essential developer workflow |
| `DEVELOPMENT_PROCESS.md` | Detailed 6-phase development process |
| `docs/wiki/` | User-facing documentation |
| `*/AGENTS.md` | Module-level architecture guides |

## License

By contributing to ClickGraph, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).

## Questions?

- Open a [GitHub issue](https://github.com/genezhang/clickgraph/issues) for bugs or feature requests
- Check existing [AGENTS.md files](src/render_plan/AGENTS.md) for module-specific architecture context
