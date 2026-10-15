---
name: outcome-loop-verify
description: This skill should be used when the user asks to "verify the outcome loop", "run the E2E gates", "check everything before pushing clickgraph", or "prove the outcome-loop change is safe". Runs the full end-to-end verification gate for ClickGraph outcome-loop work.
---

# Outcome-Loop Verify

Run the full end-to-end verification gate. Execute steps in order; stop at
the first red gate and fix before continuing. Never push with a red gate.

Execute `scripts/verify.sh` from the repo root to run gates G1-G5
automatically. It prints PASS/FAIL per gate and exits non-zero on failure.

```bash
./skills/outcome-loop-verify/scripts/verify.sh
```

## Gates

1. **G1 format** — `cargo fmt --check` is clean.
2. **G2 lint** — no NEW clippy warnings in changed files (`cargo clippy`
   baseline may carry pre-existing warnings on newer toolchains; compare
   before/after on `git stash`).
3. **G3 unit** — `cargo test --lib` passes except the documented
   pre-existing failure list in `references/known-failures.md`.
4. **G4 integration** — translate-seam tests plus the example tests pass;
   corpus sweep shows zero content mismatches (stale-golden entries are
   pre-existing).
5. **G5 examples** — `outcome_loop_example_tests` pass; every Cypher block
   in `docs/design/OUTCOME_RECIPES.md` marked tested still translates.
6. **G6 evidence** — any performance claim ships with EXPLAIN + timings
   from a live ClickHouse run, or the claim is removed. See
   `references/evidence-protocol.md`.
7. **G7 docs** — spec, recipes, README, CHANGELOG updated together.
8. **G8 push rule** — push to the fork only when G1-G7 are all green.

## References

- Known pre-existing failures: see `references/known-failures.md`.
- Live-ClickHouse evidence protocol: see `references/evidence-protocol.md`.
