# Known pre-existing failures (not caused by outcome-loop work)

Verified by stashing all changes and re-running on clean `main`:

- `bound_plan::write::tests::a_source_backed_label_is_not_writable` —
  fails on clean main (write-path test, unrelated to read/parser/render
  changes). Do not "fix" it inside an outcome-loop change.
- `corpus_sweep::corpus_sweep` — fails on clean main with only
  "STALE golden of no corpus entry" entries (stale files, no content
  mismatches). A red flag is any NON-stale mismatch line.
- `cargo clippy -- -D warnings` on newer toolchains reports pre-existing
  lints in untouched files (e.g. `variable_length_cte.rs`). Compare
  before/after; only new warnings in changed files block.
