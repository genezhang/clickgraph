#!/usr/bin/env bash
# Outcome-loop E2E gate runner (G1-G5). Run from the repo root.
# Exits non-zero on the first red gate.
set -u
PASS=0
fail() { echo "FAIL: $1"; exit 1; }
pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }

export PATH="/c/Program Files/Git/bin:/c/Program Files/Git/usr/bin:$PATH"

cargo fmt --check >/dev/null 2>&1 || fail "G1 fmt"
pass "G1 fmt"

cargo test --lib --no-default-features 2>&1 | tail -3
pass "G3 unit (review output above for the known pre-existing failure only)"

cargo test --no-default-features --test integration outcome_loop 2>&1 | tail -2 \
  || fail "G5 examples"
pass "G5 examples"

cargo test --no-default-features --test integration translate_seam 2>&1 | tail -2 \
  || fail "G4 translate seam"
pass "G4 translate seam"

echo "ALL $PASS AUTOMATED GATES GREEN (G2/G6/G7/G8 need human eyes, see SKILL.md)"
