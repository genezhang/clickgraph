# SOLID contract for outcome-loop code

- **SRP:** pure builders separate from I/O senders; scoring separate from
  serving; tests per unit, not per file.
- **OCP:** dialect differences through `FunctionMapper` / `SqlDialect`;
  new behavior via schema and env flags, never new core branches.
- **LSP:** every dialect mapper honors the shared trait; Databricks paths
  must stay valid (nothing ClickHouse-only may leak).
- **ISP:** hooks expose build/send only; callers never import internals.
- **DIP:** depend on `get_current_*` accessors and traits, never on
  process env or concrete config inside render/emit code.
- No new dependencies without a justification entry in the PR body.
  `tokio`, `reqwest`, `serde_json` already cover hook needs.
