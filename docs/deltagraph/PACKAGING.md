# Packaging ClickGraph and DeltaGraph

How the two server binaries are built, packaged, and selected by a user.
This is the distribution reference for a release; for the developer build/run
walkthrough see [QUICKSTART.md](./QUICKSTART.md).

---

## TL;DR

- **One codebase, two binaries, one Cargo feature.** `clickgraph` (ClickHouse)
  and `deltagraph` (Databricks) are the same server; the `databricks` Cargo
  feature switches on the Databricks executor + Spark-SQL dialect.
- **Preview packaging model: one Docker image containing BOTH binaries**, plus
  **separate release tarballs** (one per binary per platform).
- **A user chooses at runtime**, not at install time — pick the binary
  (`clickgraph` vs `deltagraph`), or run `clickgraph --databricks`.

---

## 1. How the binaries relate

| | `clickgraph` | `deltagraph` |
|---|---|---|
| Backend | ClickHouse / chdb | Databricks SQL Warehouse |
| Cargo package | `clickgraph` | `clickgraph` (bin `src/bin/deltagraph.rs`) |
| Feature gate | none (default build) | `required-features = ["databricks"]` |
| Server code | `clickgraph::server::run_with_config` | **same** — `deltagraph` is a thin wrapper that forces `databricks: true` |
| Default ports | HTTP 7475 / Bolt 7687 | HTTP 7475 / Bolt 7687 |
| Neo4j compat mode | opt-in (`--neo4j-compat-mode`) | **on by default** (that's the headline demo) |

`deltagraph` deliberately does not implement its own server loop — it re-uses
the shared server so Bolt fixes, query cache, and observability reach both
binaries automatically.

### The `databricks` feature is cheap and chdb-free

The feature is a **marker** that enables `#[cfg(feature = "databricks")]` code
paths (the executor in `src/executor/databricks_sql.rs` and the Spark-SQL
dialect routing). `reqwest` is already a non-optional dependency, so enabling it
adds **no heavy dependencies**.

Verified (2026-08-09): building `--features databricks` does **not** pull in
`chdb`/`libchdb`. Neither binary links `libchdb`; both are ~24 MB. This matters
because it means `deltagraph` **cross-compiles to Windows and macOS** exactly
like `clickgraph` — the `embedded` feature (chdb, no Windows binary) is the one
that can't cross-compile, and `databricks` is independent of it.

```bash
# proof
cargo tree -p clickgraph --features databricks -e no-dev | grep -i chdb   # (empty)
ldd target/release/deltagraph | grep -i chdb                              # (empty)
```

---

## 2. How a user chooses: clickgraph, deltagraph, or both

Selection is a **runtime** decision. There are three equivalent ways in:

**A. Run the binary you want** (native install / tarball)
```bash
clickgraph  --http-port 7475 --bolt-port 7687   # ClickHouse backend
deltagraph  --http-port 7476 --bolt-port 7688   # Databricks backend
```
Both can run at once on different ports — that's the "or both" case (e.g. the
Neo4j Browser demo flipping between `bolt://localhost:7687` and `:7688`).

**B. Docker: pick the entrypoint** (one image, both binaries)
```bash
# ClickHouse (default entrypoint)
docker run -p 7475:7475 -p 7687:7687 \
  -e CLICKHOUSE_URL=http://clickhouse:8123 \
  -e GRAPH_CONFIG_PATH=/schema.yaml -v ./schema.yaml:/schema.yaml \
  genezhang/clickgraph:latest

# Databricks — override the entrypoint to the other binary
docker run -p 7475:7475 -p 7687:7687 \
  -e DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com \
  -e DATABRICKS_WAREHOUSE_ID=xxxx -e DATABRICKS_TOKEN=dapiXXXX \
  -e DATABRICKS_CATALOG=workspace \
  -e GRAPH_CONFIG_PATH=/schema.yaml -v ./schema.yaml:/schema.yaml \
  --entrypoint /usr/local/bin/deltagraph \
  genezhang/clickgraph:latest
```

**C. Single binary + flag** — `clickgraph --databricks` behaves like
`deltagraph` (reads the `DATABRICKS_*` env). `deltagraph` exists so the compat
defaults and command name match the docs/demo; both routes execute the same
code.

### Backend configuration (env)

| Binary | Required env | Optional |
|---|---|---|
| `clickgraph` | `CLICKHOUSE_URL`, `GRAPH_CONFIG_PATH` | `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` |
| `deltagraph` | `DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_TOKEN`, `GRAPH_CONFIG_PATH` | `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA` |

The Databricks PAT is **env-only** — never accepted on the command line (it
would leak via `ps` / shell history).

---

## 3. Build commands

```bash
# ClickHouse server only (today's default)
cargo build --release --bin clickgraph

# Databricks server only
cargo build --release --features databricks --bin deltagraph

# BOTH in one pass (what the Docker image does)
cargo build --release --features databricks --bin clickgraph --bin deltagraph
```

Building `clickgraph` **with** `--features databricks` is harmless: the
Databricks code is compiled in but inert unless `--databricks` / the
`deltagraph` wrapper turns it on.

---

## 4. Docker image (one image, both binaries)

Changes to `Dockerfile` (builder + runtime stages):

```dockerfile
# --- builder stage: add the feature and the deltagraph bin ---
RUN cargo build --release --features databricks --bin clickgraph --bin deltagraph && \
    cargo build --release -p clickgraph-client --bin clickgraph-client

RUN strip /app/target/release/clickgraph && \
    strip /app/target/release/deltagraph && \
    strip /app/target/release/clickgraph-client

# --- runtime stage: copy both server binaries ---
COPY --from=builder /app/target/release/clickgraph      /usr/local/bin/clickgraph
COPY --from=builder /app/target/release/deltagraph      /usr/local/bin/deltagraph
COPY --from=builder /app/target/release/clickgraph-client /usr/local/bin/clickgraph-client

RUN chown -R clickgraph:clickgraph /app && \
    chmod +x /usr/local/bin/clickgraph /usr/local/bin/deltagraph /usr/local/bin/clickgraph-client

# ENTRYPOINT stays clickgraph; deltagraph is reached via --entrypoint (§2.B)
ENTRYPOINT ["/usr/local/bin/clickgraph"]
```

`docker-publish.yml` needs **no change** — it already builds `./Dockerfile` for
`linux/amd64,linux/arm64` on tag/release and pushes `genezhang/clickgraph:{latest,vX.Y.Z}`.
The image simply gains the `deltagraph` binary.

**Trade-off accepted:** the `clickgraph` binary inside the image carries inert
Databricks code. Negligible (~no size change; feature adds no deps). If strict
purity is ever wanted, split to two images at GA (`genezhang/deltagraph`).

---

## 5. Release binary tarballs

`.github/workflows/release.yaml` builds a cross-platform matrix
(linux-gnu, windows-msvc, apple x86_64, apple aarch64) and today ships
`clickgraph` + `clickgraph-client` tarballs. Add `deltagraph` as a **third
artifact**:

- **Keep the `clickgraph` tarball clean** (built *without* the feature, exactly
  as today) — a ClickHouse user's download stays byte-for-byte unchanged and
  carries no Databricks code.
- **Add a separate `deltagraph` build pass** with `--features databricks --bin deltagraph`,
  tar it as `deltagraph-<platform>.tar.gz` (`.zip` on Windows), upload alongside.

Because the feature is chdb-free, `deltagraph` builds on **all four** matrix
targets — no platform exclusions needed.

Sketch (Linux branch; mirror for the Windows/macOS branches):
```yaml
# existing: build clickgraph + clickgraph-client (unchanged, no feature)
args: "--locked --release -p clickgraph -p clickgraph-client"

# NEW: second pass for deltagraph
- name: Build deltagraph (databricks)
  run: cargo build --locked --release --features databricks --bin deltagraph --target ${{ matrix.target }}
- name: Package deltagraph
  run: tar -C target/${{ matrix.target }}/release -czf \
         dist/${{ matrix.name }}/deltagraph-${{ matrix.name }}.tar.gz deltagraph
- name: Upload deltagraph binary
  uses: softprops/action-gh-release@v2
  with:
    files: dist/${{ matrix.name }}/deltagraph-${{ matrix.name }}.*
```

Result — release assets per platform:
```
clickgraph-<platform>.tar.gz          # ClickHouse server (unchanged)
clickgraph-client-<platform>.tar.gz   # REPL (unchanged)
deltagraph-<platform>.tar.gz          # NEW — Databricks server
```

---

## 6. Out of preview scope

- **Language bindings (`clickgraph-py`, `clickgraph-go`).** These consume the
  FFI (`Database::open_databricks` exists) but are sql_only/remote today;
  packaging live Databricks execution through the bindings for distribution is a
  separate, larger effort — not in the preview.
- **`cg` CLI Databricks execution.** `cg --dialect databricks` already
  translates and (with `--features databricks`) executes, but `cg` is
  agent/script tooling, distributed separately from the server preview.
- **`MERGE`/writes, OAuth M2M auth, external-link result chunks, live-warehouse
  perf/soak** — GA items, tracked in [GA_READINESS.md](./GA_READINESS.md).

---

## 7. Preview checklist

- [ ] `Dockerfile`: build with `--features databricks`, add `deltagraph` to
      build/strip/copy/chmod (§4).
- [ ] `release.yaml`: add the `deltagraph` build+package+upload steps to each
      matrix branch (§5).
- [ ] Smoke: `docker run --entrypoint /usr/local/bin/deltagraph …` answers a
      Bolt query against a live warehouse (verified manually 2026-08-09).
- [ ] README/QUICKSTART: link this doc from the install section.
- [ ] Tag `vX.Y.Z-preview.N` → `docker-publish.yml` + `release.yaml` fire.
